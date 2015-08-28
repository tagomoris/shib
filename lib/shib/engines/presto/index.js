var Client = require('presto-client').Client
  , JSONbig = require('json-bigint');

// check interval for real queries, not system queries
var BLOCK_CHECK_INTERVAL = 1000; // 1sec

var jobname_queryid_map = {};

var Executer = exports.Executer = function(conf, logger){
  if (conf.name !== 'presto')
    throw "executer name mismatch for presto:" + conf.name;
  if (!conf.host)
    throw "host MUST be specified for presto executer";
  if (!conf.port)
    throw "port MUST be specified for presto executer";
  if (!conf.catalog)
    throw "catalog MUST be specified for presto executer";

  this.logger = logger;
  this._client = new Client({
    host: conf.host,
    port: conf.port,
    user: conf.user,
    catalog: conf.catalog,
    jsonParser: JSONbig
  });
};

Executer.prototype.end = function(){
  // Nothing to do for HTTP API :-)
};

Executer.prototype.supports = function(operation){
  switch (operation) { // "executer" methods
  case 'jobname':
  case 'setup':
  case 'databases':
  case 'tables':
  case 'partitions':
  case 'describe':
  case 'execute':
    return true;
  }
  throw "unknown operation name (for presto.Executer):" + operation;
};

Executer.prototype.jobname = function(queryid) {
  return 'shib-presto-' + queryid;
};

Executer.prototype.setup = function(setups, callback){
  // presto engine currently does not support 'setup', because of lack of UDFs
  callback(null);
};

Executer.prototype.databases = function(callback){
  this._client.execute({query:'show schemas', schema:'dummy'}, function(err, data){
    if (err) { callback(err); return; }
    // [ [ 'default' ], [ 'information_schema' ], [ 'sys' ] ]
    // database names may be always string...
    var results = [];
    data.forEach(function(row){
      var dbname = row[0];
      if (dbname !== 'information_schema' && dbname !== 'sys' )
        results.push(dbname);
    });
    callback(null, results);
  });
};

Executer.prototype.tables = function(dbname, callback){
  this._client.execute({query: 'show tables', schema: dbname}, function(err, data){
    if (err) { callback(err); return; }
    // [ [ 'table1' ], [ 'table2' ], ... ]
    // table names may be always string...
    var results = data.map(function(row){ return row[0]; });
    callback(null, results);
  });
};

Executer.prototype.partitions = function(dbname, tablename, callback){
  var client = this._client;
  var show_partitions_query = 'show partitions from ' + tablename;
  client.execute({query: 'show columns from ' + tablename, schema: dbname}, function(err, data){
    if (err) { callback(err); return; }
    // [ [ 'fieldname', 'typename', boolean_null, boolean_partition_key ], ... ]
    // partition names may be always string...
    var partitionKeys = [];
    data.forEach(function(row){
      if (row[3])
        partitionKeys.push(row[0]);
    });
    client.execute({query: show_partitions_query, schema: dbname}, function(err, data){
      if (err) { callback(err); return; }
      // data: [ [ 'partkey1_value', 'partkey2_value' ], ... ]
      // expected: ['f1=va1/f2=vb1', 'f1=va1/f2=vb2']
      var results = [];
      data.forEach(function(row){
        var part = row.map(function(v,i){ return partitionKeys[i] + '=' + String(v); }).join('/');
        results.push( part );
      });
      callback(null, results);
    });
  });
};

Executer.prototype.describe = function(dbname, tablename, callback){
  // presto "show columns from ..." does not return column comments in hive metastore
  // schema info members may not include numeric values
  this._client.execute({query:'show columns from ' + tablename, schema: dbname}, function(err, data){
    if (err) { callback(err); return; }
    // data:     [ [ 'fieldname', 'typename', boolean_null, boolean_partition_key ], ... ]
    // expected: [ [ 'fieldname', 'type', 'comment' ], ... ]
    var results = data.map(function(row){ return [ row[0], row[1], (row[3] ? 'partition key' : '') ]; });
    callback(null, results);
  });
};

Executer.prototype.execute = function(jobname, dbname, query, callback){
  var client = this._client;

  var fetcher = new Fetcher(client);

  var state_callback = function(e, query_id, stats){
    jobname_queryid_map[jobname] = query_id;
  };
  var columns_callback = function(e, columns){
    fetcher._cache.schema = columns;
  };
  var data_callback = function(e, data){
    fetcher._hasResults = true;
    var buf = []
      , len = data.length;
    for ( var i = 0 ; i < len ; i++ ) {
      // data may contain BIGINT values ...
      // AND join does 'toString()' automatically!
      var text = '';
      var column_length = data[i].length;
      for ( var j = 0 ; j < column_length ; j++ ) {
        cell = data[i][j];
        if (typeof cell == "object") {
          text += JSON.stringify(cell);
        } else {
          text += cell;
        }
        if (j != column_length - 1) {
          text += '\t';
        }
      }
      buf.push(text);
    }
    fetcher._push( buf );
  };
  var success_callback = function(e, stats){
    fetcher._noMoreResults = true;
    delete jobname_queryid_map[jobname];
  };
  var error_callback = function(e){
    delete jobname_queryid_map[jobname];
    if (! fetcher._rpcError) // only first error is stored
      fetcher._rpcError = e;
  };

  var opts = {
    query:  query,
    schema: dbname || 'default',
    state: state_callback,
    columns: columns_callback,
    data:    data_callback,
    success: success_callback,
    error:   error_callback
  };
  client.execute(opts);
  callback(null, fetcher);
};

var Fetcher = function(client){
  this._client = client;

  this._hasResults = false;
  this._noMoreResults = false;

  this._rpcError = null;
  this._cache = { data: [], schema: null };

  this._push = function(data) {
    this._cache.data = this._cache.data.concat(data);
  };

  this._waitComplete = function(callback) {
    var self = this;
    var check = function() {
      if (self._rpcError)
        callback(self._rpcError);
      else if (self._hasResults)
        callback(null);
      else if (self._noMoreResults)
        callback(null);
      else
        setTimeout(check, BLOCK_CHECK_INTERVAL);
    };
    check();
  };

  this.schema = function(callback){
    /*
     * schema(callback): callback(err, schema)
     *  schema: {fieldSchemas: [{name:'fieldname1'}, {name:'fieldname2'}, {name:'fieldname3'}, ...]}
     *  //?? schema: [{name:'fieldname1'}, {name:'fieldname2'}, ...]
     */
    var self = this;
    this._waitComplete(function(err){
      if (err) { callback(err); return; }
      // self._cache.schema: [ { name: "username", type: "varchar" }, { name: "cnt", type: "bigint" } ]
      callback(null, self._cache.schema);
    });
  };

  this.fetch = function(num, callback){
    if (!num) {
      this._fetchAll(callback);
      return;
    }

    var self = this;

    if (self._cache.data.length < 1 && self._noMoreResults) {
      // if (rows === null || rows.length < 1 || (rows.length == 1 && rows[0].length < 1)) {
      // end of fetched rows
      callback(null, null);
      return;
    }

    var buf = [];

    var fill = function() {
      if (self._rpcError) { callback(self._rpcError); return; }
      var chunk = self._cache.data.splice(0, num - buf.length);

      if (chunk.length < 1) {
        if (self._noMoreResults) {
          var tmpbuf = buf;
          buf = [];
          callback(null, tmpbuf); // if tmpbuf is empty, this is end of fetching
          return;
        }
        else {
          setTimeout(fill, BLOCK_CHECK_INTERVAL);
          return;
        }
      }

      buf = buf.concat(chunk);
      if (buf.length >= num || self._noMoreResults) {
        var fullchunk = buf;
        buf = [];
        callback(null, fullchunk);
      }
      else
        setTimeout(fill, BLOCK_CHECK_INTERVAL);
    };

    this._waitComplete(function(err){
      if (err) { callback(err); return; }
      fill();
    });
  };

  this._fetchAll = function(callback) {
    var self = this;
    var check = function() {
      if (self._rpcError)
        callback(self._rpcError);
      else if (self._noMoreResults)
        callback(null, self._cache.data);
      else
        setTimeout(check, BLOCK_CHECK_INTERVAL);
    };
    check();
  };
};

var Monitor = exports.Monitor = function(conf){
  if (conf.name !== 'presto')
    throw "executer name mismatch for presto:" + conf.name;
  if (!conf.host)
    throw "host MUST be specified for presto executer";
  if (!conf.port)
    throw "port MUST be specified for presto executer";
  if (!conf.catalog)
    throw "catalog MUST be specified for presto executer";

  this._client = new Client({
    host: conf.host,
    port: conf.port,
    user: conf.user,
    catalog: conf.catalog
  });
};

Monitor.prototype.end = function(){
};

Monitor.prototype.supports = function(operation){
  switch (operation) { // "monitor" methods
  case 'status':
  case 'kill':
    return true;
  }
  throw "unknown operation name (for presto.Monitor):" + operation;
};

function convertStatus(jobname, status) {
  /*
var status = {
  "queryId" : "20140214_083451_00012_9w6p5",
  "session" : {
    "user" : "www",
    "source" : "presto-cli",
    "catalog" : "hive",
    "schema" : "default",
    "remoteUserAddress" : "127.0.0.1",
    "userAgent" : "StatementClient/0.56",
    "startTime" : 1392366891516
  },
  "state" : "RUNNING",
  "self" : "http://10.0.0.1:8080/v1/query/20140214_083451_00012_9w6p5",
  "fieldNames" : [ "cnt" ],
  "query" : "select count(*) as cnt from tablename where date_time LIKE \u00272014020%\u0027",
  "queryStats" : {
    "createTime" : "2014-02-14T17:34:51.518+09:00",
    "executionStartTime" : "2014-02-14T17:34:52.008+09:00",
    "lastHeartbeat" : "2014-02-14T17:35:10.048+09:00",
    "elapsedTime" : "18.53s",
    "queuedTime" : "2.21ms",
    "analysisTime" : "482.62ms",
    "distributedPlanningTime" : "760.83us",
    "totalPlanningTime" : "485.67ms",
    "totalTasks" : 9,
    "runningTasks" : 9,
    "completedTasks" : 0,
    "totalDrivers" : 43631,
    "queuedDrivers" : 38,
    "runningDrivers" : 1111,
    "completedDrivers" : 42482,
    "totalMemoryReservation" : "0B",
    "totalScheduledTime" : "3.30h",
    "totalCpuTime" : "13.36m",
    "totalUserTime" : "12.07m",
    "totalBlockedTime" : "12.35s",
    "rawInputDataSize" : "14.06GB",
    "rawInputPositions" : 602557401,
    "processedInputDataSize" : "9.98GB",
    "processedInputPositions" : 594796027,
    "outputDataSize" : "0B",
    "outputPositions" : 0
    },
    "outputStage" : {
      "stageId" : "20140214_083451_00012_9w6p5.0",
      "state" : "RUNNING",
      "self" : "http://10.0.0.1:8080/v1/stage/20140214_083451_00012_9w6p5.0",
      "plan" : { ... },
      "tupleInfos" : [ "bigint" ],
      "stageStats" : {
        "schedulingComplete" : "2014-02-14T17:34:52.008+09:00",
      ...
}
var returnedValus = {
  jobid: '20140214_083451_00012_9w6p5',
  name: 'shib-presto-3578d8d4f5a1812de7a7714f5b108776',
  priority: 'unknown',
  state: 'RUNNING',
  trackingURL: 'http://10.0.0.1:8080/v1/query/20140214_083451_00012_9w6p5?pretty',
  startTime: 'Thu Apr 11 2013 16:06:40 (JST)',
  mapComplete: null,
  reduceComplete: null,
  complete: 80
};
   */
  if (status === undefined) {
    return null;
  }
  var retval = {};

  retval['jobid'] = status['queryId'];
  retval['name'] = jobname;
  retval['priority'] = 'unknown';
  retval['state'] = status['state'];
  retval['trackingURL'] = status['self'] + '?pretty';

  retval['startTime'] = status['queryStats']['createTime'];
  
  var splits = status.queryStats.totalDrivers;
  var completedSplits = status.queryStats.completedDrivers;
  retval['complete'] = parseInt(completedSplits * 1000 / splits) / 10;

  return retval;
};

Monitor.prototype.status = function(jobname, callback){
  var queryid = jobname_queryid_map[jobname];
  if (! queryid) {
    callback({message:"query already expired (maybe completed)"}, null);
    return;
  }
  this._client.query(queryid, function(err, data){
    if (err) { callback(err); return; }
    callback(null, convertStatus(jobname, data));
  });
};

Monitor.prototype.kill = function(query_id, callback){
  this._client.kill(query_id, callback);
};
