var Query = require('./query').Query,
    Result = require('./result').Result;

var async = require('async');

var localdiskstore = require('./localdiskstore'),
    LocalDiskStoreError = localdiskstore.LocalStoreError;
var engine = require('./engine');

var FETCH_LINES_DEFAULT = 1000;

var STATUS_LABEL_RUNNING = "running",
    STATUS_LABEL_DONE = "done",
    STATUS_LABEL_RERUNNING = "re-running";

var Client = exports.Client = function(args){
  this._conf = args;
  this._localstore = undefined;
  this._engine = undefined;

  this._default_database = undefined;
  if (this._conf.executer.support_database) {
      this._default_database = (this._conf.executer.default_database || 'default');
  }
  this._setup_queries = this._conf.setup_queries || [];
  this._fetch_lines = this._conf.fetch_lines || FETCH_LINES_DEFAULT;

  this.localStore(); // to initialize sqlite3 database
};

Client.prototype.localStore = function(){
  if (this._localstore) {
    return this._localstore;
  }
  this._localstore = new localdiskstore.LocalDiskStore(this._conf.storage.datadir);
  return this._localstore;
};

Client.prototype.engine = function(){
  if (this._engine) {
    return this._engine;
  }
  this._engine = new engine.Engine(this._conf.executer, this._conf.monitor, this._conf.query_timeout);
  return this._engine;
};

Client.prototype.end = function(){
  var client = this;
  if (this._localstore) {
    this._localstore.close();
    this._localstore = undefined;
  }
  if (this._engine) {
    this._engine.close();
    this._engine = undefined;
  }
};

var pad = function(n){return n < 10 ? '0'+n : n;};
var historyKey = function(){
  var d = new Date();
  return '' + d.getFullYear() + pad(d.getMonth() + 1);
};

var jobName = function(queryid){ return 'shib-' + queryid; };


function error_callback(name, t, callback, err, data){
  console.log({name:name, err:err, data:data});
  if (! callback) return;
  if (data && data['ERROR'])
    err.message += ' ERROR:' + data['ERROR'];
  callback.apply(t, [err]);
};

Client.prototype.recentQueries = function(num, callback){
  var client = this;
  this.localStore().recentQueries(num, function(err, list){
    if (err) { error_callback('recentQueries', client, callback, err); return; }
    callback.apply(client, [err, list]);
  });
};

Client.prototype.addRecent = function(query, callback){
  var client = this;
  this.localStore().addRecent(historyKey(), query.queryid, function(err){
    if (err) { error_callback('addRecent', client, callback, err); return; }
    if (callback)
      callback.apply(client, [err]);
  });
};

Client.prototype.getQuery = function(queryid, callback){
  var client = this;
  this.localStore().query(queryid, function(err, data){
    if (err) { error_callback('getQuery', client, callback, err, data); return; }
    callback.apply(client, [err, data]);
  });
};
Client.prototype.query = Client.prototype.getQuery;

Client.prototype.getQueries = function(queryids, callback){
  var client = this;
  this.localStore().queries(queryids, function(err, list){
    if (err) { error_callback('getQueries', client, callback, err, list); return; }
    callback.apply(client, [err, list]);
  });
};
Client.prototype.queries = Client.prototype.getQueries;

Client.prototype.updateQuery = function(query, callback) {
  var client = this;
  this.localStore().updateQuery(query, function(err){
    if (err) { error_callback('updateQuery', client, callback, err); return; }
    if (callback)
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
      if (err) { error_callback('createQuery', client, callback, err); return; }
      callback.apply(client, [err, query]);
    });
  });
  // }
  // catch (e) {
  //   error_callback('createQuery catch', client, callback, e);
  // }
};

Client.prototype.deleteQuery = function(queryid, callback){
  var client = this;
  this.localStore().deleteQuery(queryid, function(err){
    if (err) { error_callback('deleteQuery', client, callback, err); return; }
    callback.apply(client, [err]);
  });
};

Client.prototype.getResult = function(resultid, callback){
  var client = this;
  this.localStore().result(resultid, function(err, result){
    if (err) { error_callback('getResult', client, callback, err, result); return; }
    callback.apply(client, [err, result]);
  });
};
Client.prototype.result = Client.prototype.getResult;

Client.prototype.getResults = function(resultids, callback){
  var client = this;
  this.localStore().results(resultids, function(err, list){
    if (err) { error_callback('getResults', client, callback, err, list); return; }
    callback.apply(client, [err, list]);
  });
};
Client.prototype.results = Client.prototype.getResults;

Client.prototype.setResult = function(result, callback){
  var client = this;
  this.localStore().insertResult(result, function(err){
    if (err && err.code === 'SQLITE_CONSTRAINT') {
      client.localStore().updateResult(result, function(err){
        if (err) { error_callback('setResult(update)', client, callback, err); return; }
        if (callback)
          callback.apply(client, [err]);
      });
      return;
    }
    if (err) { error_callback('setResult', client, callback, err); return; }
    if (callback)
      callback.apply(client, [err]);
  });
};

Client.prototype.getResultData = function(resultid, callback){
  var client = this;
  this.localStore().readResultData(resultid, function(err, data){
    if (err) { error_callback('getResultData', client, callback, err, data); return; }

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
    if (err) { error_callback('getRawResultData', client, callback, err, data); return; }
    callback.apply(client, [err, data]);
  });
};
Client.prototype.rawResultData = Client.prototype.getRawResultData;

Client.prototype.appendResultData = function(resultid, data, callback){
  var client = this;
  this.localStore().appendResultData(resultid, data.join("\n") + "\n", function(err){
    if (err) { error_callback('appendResultData', client, callback, err); return; }
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
  if (!query || !query.results){ callback.apply(client, ['unknown']); return; }
  if (query.results.length < 1){ callback.apply(client, ["running"]); return; }

  var resultids = query.results.reverse().map(function(v){return v.resultid;});
  this.localStore().results(resultids, function(err, results){
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

/* execute query without all of query checks, history-saving and result-caching */
Client.prototype.executeSystemStatement = function(quoted_query, callback){
  var client = this;
  client.engine().execute(null, quoted_query, function(err, data){
    if (err) { callback.apply(client, [err]); return; }
    callback.apply(client, [null, data]);
  });
};

/* select database (or no one operations done if this.database is undefined) */
Client.prototype.useDatabase = function(name, callback){
  var client = this;
  if (client.default_database === undefined) {
    callback.apply(client, [null, client]); return;
  }
  if (name === null || name === undefined) {
    name = this.default_database;
  }
  client.executeSystemStatement('use ' + name, function(err){
    if (err) { callback.apply(client, [err]); return; }
    callback.apply(client, [null, client]);
  });
};

/* get partitions of specified table */
Client.prototype.partitions = function(tablename, callback){
  var client = this;
  client.executeSystemStatement('show partitions ' + tablename, function(err, result){
    if (err) { callback.apply(client, [null]); return; }

    var partition_nodes = [];
    var treenodes = {};

    var create_node = function(partition, hasChildren){
      if (treenodes[partition])
        return treenodes[partition];
      var parts = partition.split('/');
      var leafName = parts.pop();
      var node = {title: leafName};
      if (hasChildren) {
        node.children = [];
      }
      if (parts.length > 0) {
        var parent = create_node(parts.join('/'), true);
        parent.children.push(node);
      }
      else {
        partition_nodes.push(node);
      }
      treenodes[partition] = node;
      return node;
    };

    result.forEach(function(partition){
      create_node(partition);
    });
    callback.apply(client, [null, partition_nodes]);
  });
};

/* get table schema info */
Client.prototype.describe = function(tablename, callback){
  var client = this;
  client.executeSystemStatement('describe ' + tablename, function(err, result){
    if (err) { callback.apply(client, [err]); return; }
    var rows = result.map(function(row){
      return row.split('\t');
    });
    callback.apply(client, [null, rows]);
  });
};

Client.prototype.giveup = function(query, callback){
  var client = this;

  var resultMark = function(){
    var result;
    if (query.results && query.results.length > 0)
      result = query.results[query.results.length - 1];

    if (result) {
      // if result record already generated
      var resultid = result.resultid;
      client.result(resultid, function(err, result){
        result.markAsExecuted({message: 'specified as "give up"'});
        client.setResult(result, function(err){
          if (callback)
            callback.apply(client, [err, query]);
        });
      });
    } else {
      // result record not generated yet
      var executed_at = (new Date()).toLocaleString();
      result = new Result({queryid:query.queryid, executed_at:executed_at});
      result.markAsExecuted({message: 'specified as "give up"'});
      client.setResult(result, function(err){
        query.results.push({executed_at:executed_at, resultid:result.resultid});
        client.updateQuery(query);
        if (callback)
          callback.apply(client, [null, query]);
      });
    }
  };

  var jobname = jobName(query.queryid);
  client.engine().status(jobname, function(err, jobdata){
    if (err){ // engine doesn't support 'status' or errors with other reason
      // cannot kill with no status information
      resultMark();
      return;
    }
    if (jobdata) {// killed job terminates hive query and executer calls callback with error "killed by user"
      client.engine().kill(jobdata.jobid, function(err){
        if (err) {
          console.log("Error on killing job:" + jobdata.jobid);
          console.log(err);
        }
      });
    }
    resultMark();
  });
};

Client.prototype.execute = function(query, args){
  if (! args)
    args = {};
  if (! args.scheduled)
    this.addRecent(query);

  var client = this;

  var executed_at = (new Date()).toLocaleString();
  var result = new Result({queryid:query.queryid, executed_at:executed_at});
  this.setResult(result, function(){
    query.results.push({executed_at:executed_at, resultid:result.resultid});
    client.updateQuery(query);
  
    if (args.prepare) args.prepare(query);

    var schemaRow = null;

    var resultLines = 0;
    var resultBytes = 0;
    var onerror = null;

    var jobname = jobName(query.queryid);
    client.engine().execute(jobname, query.composed(), {
      setups: this._setup_queries,
      fetchNum: this._fetch_lines,
      stopcheck: args.stopCheck,
      stop: args.stop,
      complete: function(err){
        result.markAsExecuted(onerror);
        result.lines = resultLines;
        result.bytes = resultBytes;
        client.setResult(result);

        if (onerror && args.error)
          args.error();
        else if (args.success)
          args.success();
      },
      error: function(err){
        result.markAsExecuted(err);
        client.setResult(result);
        if (args.error)
          args.error();
      },
      timeout: function(err){
        result.markAsExecuted(err);
        client.setResult(result);
        if (args.error)
          args.error();
      },
      schema: function(error, data){
        if (error){ onerror = error; return; }
        result.schema = data.fieldSchemas;
        // set to write to top of result data
        schemaRow = result.schema.map(function(f){return f.name.toUpperCase();}).join('\t');
      },
      fetch: function(error, rows, cb){
        if (error){ onerror = error; cb(); return; }
        if (schemaRow) {
          rows.unshift(schemaRow);
          schemaRow = null;
        }
        client.appendResultData(result.resultid, rows, function(err){
          if (err) { console.log(err); cb(err); throw new LocalDiskStoreError("failed to append result data"); }
          resultLines += rows.length;
          resultBytes += rows.reduce(function(prev,v){return prev + v.length + 1;}, 0);
          cb();
        });
      }
    });
  });
};

Client.prototype.detailStatus = function(queryid, callback){
  var client = this;
  this.engine().status(jobName(queryid), function(err, status){
    if (err) { callback.apply(client, [err, null]); return; }
    callback.apply(client, [null, status]);
  });
};
