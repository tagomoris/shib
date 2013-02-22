var Query = require('./query').Query,
    Result = require('./result').Result;

var async = require('async');

var thrift = require('thrift'),
    ttransport = require('thrift/lib/thrift/transport'),
    ThriftHive = require('gen-nodejs/ThriftHive');

var localdiskstore = require('./localdiskstore'),
    LocalDiskStoreError = localdiskstore.LocalStoreError;

var HuahinClient = require('./huahin_client').HuahinClient;

var STATUS_LABEL_RUNNING = "running",
    STATUS_LABEL_DONE = "done",
    STATUS_LABEL_RERUNNING = "re-running";

var HIVESERVER_READ_LINES = 100000;

var Client = exports.Client = function(args){
  this._conf = args;
  this._localstore = undefined;

  this._hiveconnection = undefined;
  this._hiveclient = undefined;

  this._huahinclient = undefined;

  this._default_database = undefined;
  if (this.conf.hiveserver.support_database) {
      this._default_database = (this.conf.hiveserver.default_database || 'default');
  }
  this._setup_queries = this._conf.setup_queries || [];
};

Client.prototype.localStore = function(){
  if (this._localstore) {
    return this._localstore;
  }
  this._localstore = localdiskstore.LocalDiskStore(this._conf.storage.datadir);
  return this._localstore;
};

Client.prototype.hiveClient = function(){
  if (this._hiveconnection && this._hiveclient) {
    return this._hiveclient;
  }
  this._hiveconnection = thrift.createConnection(
    this._conf.hiveserver.host,
    this._conf.hiveserver.port,
    {transport: ttransport.TBufferedTransport}
  );
  this._hiveclient = thrift.createClient(ThriftHive, this._hiveconnection);
  return this._hiveclient;
};

Client.prototype.huahinClient = function(){
  if (! this._conf.huahinmanager || ! this._conf.huahinmanager.enable) {
    return null;
  }
  var conf = this._conf.huahinmanager;

  if (this._huahinclient) {
    return this._huahinclient;
  }
  this._huahinclient = new HuahinClient(conf.host, conf.port);
  return this._huahinclient;
};

Client.prototype.end = function(){
  var client = this;
  if (this._localstore) {
    this._localstore.close();
    this._localstore = undefined;
  }

  if (this._hiveconnection) {
    this.hiveClient().clean(function(){
      client._hiveconnection.end();
      client._hiveconnection = client._hiveclient = undefined;
    });
  }
  this._huahinclient = undefined;
};

var pad = function(n){return n < 10 ? '0'+n : n;};
var historyKey = function(){
  var d = new Date();
  return '' + d.getFullYear() + pad(d.getMonth() + 1);
};

function error_callback(t, callback, err, data){
  if (! callback) return;
  if (data && data['ERROR'])
    err.message += ' ERROR:' + data['ERROR'];
  callback.apply(t, [err]);
};

Client.prototype.recentQueries = function(num, callback){
  var client = this;
  this.localStore().recentQueries(num, function(err, list){
    if (err) { error_callback(client, callback, err); return; }
    callback.apply(client, [err, list]);
  });
};

Client.prototype.addRecent = function(query, callback){
  var client = this;
  this.localStore().addRecent(historyKey(), query.queryid, function(err){
    if (err) { error_callback(client, callback, err); return; }
    if (callback)
      callback.apply(client, [err]);
  });
};

Client.prototype.getQuery = function(queryid, callback){
  var client = this;
  this.localStore().query(queryid, function(err, data){
    if (err) { error_callback(client, callback, err, data); return; }
    callback.apply(client, [err, data]);
  });
};
Client.prototype.query = Client.prototype.getQuery;

Client.prototype.getQueries = function(queryids, callback){
  var client = this;
  this.localStore().queries(queryids, function(err, list){
    if (err) { error_callback(client, callback, err, list); return; }
    callback.apply(client, [err, list]);
  });
};
Client.prototype.queries = Client.prototype.getQueries;

Client.prototype.updateQuery = function(query, callback) {
  var client = this;
  this.localStore().updateQuery(query, function(err){
    if (err) { error_callback(client, callback, err); return; }
    callback.apply(client, [err]);
  });
};

Client.prototype.createQuery = function(querystring, callback){
  var client = this;
  // try {
  var seed = (new Date()).toTimeString(); // seed is not needed strictlicity
  var query = new Query({querystring:querystring, seed: seed});
  client.query(query.queryid, function(err, savedquery){
    if (!err && savedquery) { callback.apply(client, [err, savedquery]); return; }

    this.localStore().insertQuery(query, function(err){
      if (err) { error_callback(client, callback, err); return; }
      callback.apply(client, [err, query]);
    });
  });
  // }
  // catch (e) {
  //   error_callback(client, callback, e);
  // }
};

Client.prototype.deleteQuery = function(queryid, callback){
  var client = this;
  this.localStore().deleteQuery(queryid, function(err){
    if (err) { error_callback(client, callback, err); return; }
    callback.apply(client, [err]);
  });
};

Client.prototype.getResult = function(resultid, callback){
  var client = this;
  this.localStore().result(resultid, function(err, result){
    if (err) { error_callback(client, callback, err, result); return; }
    callback.apply(client, [err, result]);
  });
};
Client.prototype.result = Client.prototype.getResult;

Client.prototype.getResults = function(resultids, callback){
  var client = this;
  this.localStore().results(resultids, function(err, list){
    if (err) { error_callback(client, callback, err, list); return; }
    callback.apply(client, [err, list]);
  });
};
Client.prototype.results = Client.prototype.getResults;

Client.prototype.setResult = function(result, callback){
  var client = this;
  this.localStore().insertResult(result, function(err){
    if (err) { error_callback(client, callback, err); return; }
    callback.apply(client, [err]);
  });
};

Client.prototype.getResultData = function(resultid, callback){
  var client = this;
  this.localStore().readResultData(resultid, function(err, data){
    if (err) { error_callback(client, callback, err, data); return; }

    var list = [];
    data.split("\n").forEach(function(line){
      if (line == "")
        return;
      list.push(line.split("\t"));
    });
    callback.apply(client, [err, list]);
  });
};
Client.prototype.resultData = Client.prototype.getResultData;

Client.prototype.getRawResultData = function(resultid, callback){
  var client = this;
  this.localStore().readResultData(resultid, function(err, data){
    if (err) { error_callback(client, callback, err, data); return; }
    callback.apply(client, [err, data]);
  });
};
Client.prototype.rawResultData = Client.prototype.getRawResultData;

Client.prototype.appendResultData = function(resultid, data, callback){
  var client = this;
  this.localStore().appendResultData(resultid, data.join("\n") + "\n", function(err){
    if (err) { error_callback(client, callback, err); return; }
    if (callback)
      callback.apply(client, [err]);
  });
};

Client.prototype.getLastResult = function(query, callback){
  var client = this;
  if (query === null || query === undefined) { callback.apply(client, [undefined, null]); return; }
  if (query.results.length < 1) { callback.apply(client, [undefined, null]); return; }

  var resultids = query.results.reverse().map(function(v){ return v.resultid; });
  var funcs = resultids.map(function(id){ return function(cb){
    client.localStore().result(id, function(err, result){
      if (err || result === null || result.running()) { cb(null); return; }
      callback.apply(client, [null, result]);
      cb(id);
    });
  };});

  async.series(funcs, function(err, results){
    if (err) { return; } // break because valid result object found
    callback.apply(client, [null, null]);
  });
};

Client.prototype.status = function(query, callback){
  var client = this;
  /*
   callback argument
   running: newest-and-only query running, and result not stored yet.
   executed (done): newest query executed, and result stored.
   error: newest query executed, but done with error.
   re-running: newest query running, but older result exists.
   */
  if (query.results.length < 1) { callback.apply(client, ["running"]); return; }

  var resultids = query.results.reverse().map(function(v){return v.resultid;});
  this.localSotre().results(resultids, function(err, results){
    if (! results.every(function(element, index, array){return element !== null && element !== undefined;}))
      throw new LocalDiskStoreError("Result is null for one or more ids of: " + resultids.join(","));

    var newest = results.shift();

    if (newest.running()){
      if (results.length < 1)
        callback.apply(client, ["running"]);
      else {
        var alter = results.shift();
        if (! alter.running() && ! alter.withError())
          callback.apply(client, ["re-running"]);
        else
          callback.apply(client, ["running"]);
      }
    }
    else if (newest.withError())
      callback.apply(client, ["error"]);
    else
      callback.apply(client, ["executed"]);
  });
};

/* select database (or no one operations done if this.database is undefined) */
Client.prototype.useDatabase = function(name, callback){
  var client = this;
  if (client.default_database === undefined) {
    callback(null, client); return;
  }
  if (name === null || name === undefined) {
    name = this.default_database;
  }
  client.hiveClient().execute('use ' + name, function(err) {
    if (err) { callback(err, null); return; }
    callback(null, client);
  });
};

/* execute query without all of query checks, history-saving and result-caching */
Client.prototype.executeSystemStatement = function(quoted_query, callback){
  var client = this;
  client.hiveClient().execute(quoted_query, function(err, data){
    client.hiveClient().fetchAll(function(err, result){
      if (err) {
        callback.apply(client, [err]);
        return;
      }
      callback.apply(client, [null, result]);
    });
  });
};

Client.prototype.giveup = function(query, callback){
  var client = this;
  var result = query.results[query.results.length - 1];
  if (result === undefined){
    var executed_at = (new Date()).toLocaleString();
    result = new Result({queryid:query.queryid, executed_at:executed_at});
    result.markAsExecuted({message: 'specified as "give up"'});
    this.setResult(result, function(){
      query.results.push({executed_at:executed_at, resultid:result.resultid});
      this.updateQuery(query);
      if (callback)
        callback.apply(client, [null, query]);
    });
    return;
  }
  var resultid = result.resultid;
  this.result(resultid, function(err, result){
    result.markAsExecuted({message: 'specified as "give up"'});
    client.setResult(result, function(err){
      if (callback)
        callback.apply(client, [err, query]);
    });
  });
};

Client.prototype.setupClient = function(setups, callback){
  this.useDatabase(null, function(err, client){
    if (err) { callback(err, client); return; }
    if (setups.length < 1) { callback(null, client); return; }

    var setupQueue = setups.concat(); // shallow copy of Array
    var executeSetup = function(queue, callback){
      var q = queue.shift();
      client.hiveClient().execute(q, function(err) {
        if (err)
          callback(err, null);
        else
          client.hiveClient().fetchAll(function(err, data){
            if (queue.length > 0)
              executeSetup(queue, callback);
            else
              callback(null, client);
          });
      });
    };
    executeSetup(setupQueue, callback);
  });
};

Client.prototype.execute = function(query, args){
  if (! args)
    args = {};
  this.addRecent(query);

  var client = this;

  var executed_at = (new Date()).toLocaleString();
  var result = new Result({queryid:query.queryid, executed_at:executed_at});
  this.setResult(result, function(){
    query.results.push({executed_at:executed_at, resultid:result.resultid});
    this.updateQuery(query);
  
    if (args.prepare) args.prepare(query);

    this.setupClient(this.setup_queries, function(err, client) {
      if (err || !client) {
        if (client){
          result.markAsExecuted(err);
          client.setResult(result);
        }
        if (args.error) args.error(query);
        return;
      }
      client.hiveClient().execute(query.composed(result.resultid), function(err, data){
        if (args.broken && args.broken(query)) {
          return;
        }
        if (err) {
          result.markAsExecuted(err);
          client.setResult(result);
          if (args.error) args.error(query);
          return;
        }

        var resultkey = result.resultid;
        var resultLines = 0;
        var resultBytes = 0;
        var schemaRow = null;
        var onerror = null;

        var resultfetch = function(callback) {
          client.hiveClient().fetchN(HIVESERVER_READ_LINES, function(err, data){
            if (err){
              onerror = err;
              return;
            }
            if (data.length < 1 || data.length == 1 && data[0].length < 1){
              callback.apply(client, []);
              return;
            }
            if (schemaRow) {
              data.unshift(schemaRow);
              schemaRow = null;
            }
            client.appendResultData(resultkey, data, function(err){
              if (err)
                throw new LocalDiskStoreError("failed to append result data");
              resultLines += data.length;
              resultBytes += data.reduce(function(prev,v){return prev + v.length + 1;}, 0);
              resultfetch(callback);
            });
          });
        };
        client.hiveClient().getSchema(function(err, data){
          if (err){
            onerror = err;
            return;
          }
          result.schema = data.fieldSchemas;
          schemaRow = result.schema.map(function(f){return f.name.toUpperCase();}).join('\t');
          resultfetch(function(){
            result.markAsExecuted(onerror);
            result.lines = resultLines;
            result.bytes = resultBytes;
            client.setResult(result);

            if (onerror && args.error)
              args.error(query);
            else if (args.success)
              args.success(query);
          });
        });
      });
    });
  });
};

Client.prototype.searchJob = function(queryid, callback){
  var marker = '--- ' + queryid.substr(0,8);
  
  var client = this;

  this.huahinClient().listAll(function(err, result){
    if (err) { callback.apply(client, [err, null]); }

    var jobidlist = [];
    result.data.forEach(function(job){
      if (job.name.indexOf(marker) === 0) {
        jobidlist.push(job.jobid);
      }
    });
    callback.apply(client, [null, jobidlist[0]]);
  });
};

Client.prototype.hiveStatus = function(detail){
  /* TODO: these values will be thrown away?
{
  "groups": {
    "FileSystemCounters": {
      "FILE_BYTES_WRITTEN": 12654144,
      "HDFS_BYTES_READ": 1395562717,
      "HDFS_BYTES_WRITTEN": 212242870
    },
    "Job Counters ": {
      "Data-local map tasks": 166,
      "Failed map tasks": 1,
      "Launched map tasks": 176,
      "Rack-local map tasks": 10,
      "SLOTS_MILLIS_MAPS": 945952,
      "SLOTS_MILLIS_REDUCES": 0,
      "Total time spent by all maps waiting after reserving slots (ms)": 0,
      "Total time spent by all reduces waiting after reserving slots (ms)": 0
    },
    "Map-Reduce Framework": {
      "Map input bytes": 1395511589,
      "Map input records": 4061894,
      "Map output records": 0,
      "SPLIT_RAW_BYTES": 51128,
      "Spilled Records": 0
    },
    "org.apache.hadoop.hive.ql.exec.FileSinkOperator$TableIdEnum": {"TABLE_ID_1_ROWCOUNT": 4061894},
    "org.apache.hadoop.hive.ql.exec.FilterOperator$Counter": {
      "FILTERED": 0,
      "PASSED": 4061894
    },
    "org.apache.hadoop.hive.ql.exec.MapOperator$Counter": {"DESERIALIZE_ERRORS": 0},
    "org.apache.hadoop.hive.ql.exec.Operator$ProgressCounter": {"CREATED_FILES": 166}
  },
}
*/
  /*
   jobid, name, priority, state, jobSetup, status, jobCleanup,
   trackingURL, startTime, mapComplete, reduceComplete,
   hiveQueryId, hiveQueryString
   */
  if (detail === undefined || detail['data'] === undefined) {
    return null;
  }
  var data = detail.data;
  var status = {};
  var attrs = ['jobid','name','priority','state','jobSetup','status','jobCleanup','trackingURL','startTime','mapComplete','reduceComplete'];
  attrs.forEach(function(attr){
    status[attr] = data[attr];
  });
  status['hiveQueryId'] = (data['configuration'] || {})['hive.query.id'];
  status['hiveQueryString'] = (data['configuration'] || {})['hive.query.string'];
  return status;
};

Client.prototype.detailStatus = function(queryid, callback){
  var client = this;
  this.searchJob(queryid, function(err, jobid){
    if (err) { callback.apply(client, [err, null]); }

    this.huahinClient().detail(jobid, function(err, detail){
      if (err) { callback.apply(client, [err, null]); }

      callback.apply(client, [null, client.hiveStatus(detail)]);
    });
  });
};

Client.prototype.killJob = function(jobid, callback) {
  var client = this;
  this.huahinClient().kill(jobid, function(err, result){
    callback.apply(client, [err, result]);
  });
};

Client.prototype.killQuery = function(queryid, callback) {
  var client = this;
  this.searchJob(queryid, function(err, jobid){
    if (err) { callback.apply(client, [err, null]); }

    this.huahinClient().kill(jobid, function(err, result){
      callback.apply(client, [err, result]);
    });
  });
};