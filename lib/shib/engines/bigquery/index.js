var gcloud = require('gcloud')
  , util = require('util');

// check interval for real queries, not system queries
var BLOCK_CHECK_INTERVAL = 1000; // 1sec
var MAX_RESULTS_FOR_EACH_FETCH = 1000;

var jobname_jobid_map = {};

var Executer = exports.Executer = function(conf, logger){
  if (conf.name !== 'bigquery')
    throw "executer name mismatch for bigquery:" + conf.name;
  if (!conf.project_id)
    throw "project_id MUST be specified for bigquery executer";
  if (!conf.key_filename)
    throw "key_filename MUST be specified for bigquery executer";

  this.logger = logger;
  this._client = gcloud.bigquery({
    projectId: conf.project_id,
    keyFilename: conf.key_filename
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
  case 'describe':
  case 'execute':
    return true;
  }
  throw "unknown operation name (for bigquery):" + operation;
};

Executer.prototype.jobname = function(queryid) {
  return 'shib-bigquery-' + queryid;
};

Executer.prototype.setup = function(setups, callback){
  callback(null);
};

Executer.prototype.databases = function(callback){
  var results = []
    , pageToken = null
    , self = this;

  var processDatasets = function(datasets) {
    datasets.forEach(function(row) {
      var dbname = row.metadata.datasetReference.datasetId;
      results.push(dbname);
    });
  };

  var fetchDatesets = function() {
    self._client.getDatasets({ maxResults: MAX_RESULTS_FOR_EACH_FETCH, pageToken: pageToken }, function(err, datasets, nextQuery){
      if (err) { callback(err); return; }

      processDatasets(datasets);

      if (nextQuery) {
        pageToken = nextQuery.pageToken;
        setTimeout(fetchDatesets, 0);
      } else {
        callback(null, results);
      }
    });
  };
  fetchDatesets();
};

Executer.prototype.tables = function(dbname, callback){
  var results = []
    , pageToken = null
    , self = this
    , dataset = self._client.dataset(dbname);

  var processTables = function(tables) {
    tables.forEach(function(row) {
      var table = row.metadata.tableReference.tableId;
      results.push(table);
    });
  };

  var fetchTables = function() {
    dataset.getTables({ maxResults: MAX_RESULTS_FOR_EACH_FETCH, pageToken: pageToken }, function(err, tables, nextQuery){
      if (err) { callback(err); return; }

      processTables(tables);

      if (nextQuery) {
        pageToken = nextQuery.pageToken;
        setTimeout(fetchTables, 0);
      } else {
        callback(null, results);
      }
    });
  };
  fetchTables();
};

Executer.prototype.partitions = function(dbname, tablename, callback){
  // bigquery engine does not support 'partitions'
  callback(null, []);
};

Executer.prototype.describe = function(dbname, tablename, callback){
  var dataset = this._client.dataset(dbname);
  var table = dataset.table(tablename);

  table.getMetadata(function(err, metadata){
    if (err) { callback(err); return; }

    var results = [];
    var fields = metadata.schema.fields;
    fields.forEach(function(field){
      var name = field.name;
      var type = field.type;
      var comment = field.description ? field.description : '';
      results.push([name, type, comment]);
    });
    callback(null, results);
  });
};

Executer.prototype.execute = function(jobname, dbname, query, callback){
  var client = this._client;

  var fetcher = new Fetcher(client, jobname);

  var error_callback = function(e){
    delete jobname_jobid_map[jobname];
    if (!fetcher._rpcError) // only first error is stored
      fetcher._rpcError = e;
  };

  client.startQuery(query, function(err, job) {
    if (err) { error_callback(err); return; }
    jobname_jobid_map[jobname] = job.id;
  });

  callback(null, fetcher);
};

var Fetcher = function(client, jobname){
  this._client = client;
  this._jobname = jobname;

  this._hasResults = false;
  this._noMoreResults = false;
  this._rpcError = null;
  this._nextQuery = null;

  this._cache = { data: [], schema: null };

  this._processColumn = function(data){
    var columns = [];
    if (data.length > 0) {
      var first = data[0];
      Object.keys(first).forEach(function(key){
        columns.push({ name: key });
      });
    }
    this._cache.schema = columns;
    this._hasResults = true;
  };

  this._processData = function(data){
    var buf = []
      , len = data.length
      , schema = this._cache.schema
      , schemaLen = schema.length;

    for ( var i = 0 ; i < len ; i++ ) {
      var row = [];
      for (var j = 0 ; j < schemaLen; j++ ) {
        row.push(data[i][schema[j].name]);
      }
      buf.push( row.join('\t') );
    }
    this._push( buf );
  };

  this._push = function(data) {
    this._cache.data = this._cache.data.concat(data);
  };

  this._waitComplete = function(callback) {
    if (this._hasResults || this._rpcError) {
      if (this._rpcError)
        callback(this._rpcError);
      else
        callback(null);
      return;
    }

    var self = this;
    var check = function() {
      var jobId = jobname_jobid_map[self._jobname];
      if (jobId) {
        var job = self._client.job(jobId);
        job.getQueryResults({
          maxResults: MAX_RESULTS_FOR_EACH_FETCH,
          pageToken: self._nextQuery ? self._nextQuery.pageToken : null
        }, function (err, rows, nextQuery) {
          if (err) { callback(err); return; }

          if (!self._hasResults) {
            self._processColumn(rows);
          }
          self._processData(rows);

          if (!nextQuery) {
            self._nextQuery = null;
            self._noMoreResults = true;
            delete jobname_jobid_map[self._jobname];
            return callback(null);
          }

          self._nextQuery = nextQuery;
          setTimeout(check, BLOCK_CHECK_INTERVAL);
        });
      } else {
        setTimeout(check, BLOCK_CHECK_INTERVAL);
      }
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
  if (conf.name !== 'bigquery')
    throw "executer name mismatch for bigquery:" + conf.name;
  if (!conf.project_id)
    throw "project_id MUST be specified for bigquery executer";
  if (!conf.key_filename)
    throw "key_filename MUST be specified for bigquery executer";

  this._client = gcloud.bigquery({
    projectId: conf.project_id,
    keyFilename: conf.key_filename
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
  throw "unknown operation name (for bigquery.Monitor):" + operation;
};

function convertStatus(jobname, status) {
  if (status === undefined) {
    return null;
  }

  var retval = {};

  // https://cloud.google.com/bigquery/docs/reference/v2/jobs
  retval['jobid'] = status['jobReference']['jobId'];
  retval['name'] = jobname;
  retval['priority'] = status['configuration']['priority'] || 'INTERACTIVE';
  retval['state'] = status['status']['state'];
  retval['trackingURL'] = status['selfLink'];

  retval['startTime'] = new Date(parseInt(status['statistics']['creationTime']));
  retval['complete'] = (retval['state'] === 'DONE' ? 100 : 0);

  return retval;
}

Monitor.prototype.status = function(jobname, callback){
  var jobId = jobname_jobid_map[jobname];
  if (!jobId) {
    callback({message:"job already expired (maybe completed)"}, null);
    return;
  }
  var job = this._client.job(jobId);
  job.getMetadata(function(err, data){
    if (err) { callback(err); return; }
    callback(null, convertStatus(jobname, data));
  });
};

Monitor.prototype.kill = function(query_id, callback){
  callback(null);
};
