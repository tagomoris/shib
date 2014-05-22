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
  var self = this;

  this._conf = args;
  this._localstore = undefined;

  /*
   * engine configurations defaults
   *   fetch_lines: 1000,
   *   query_timeout: null,
   *   setup_queries: [],
   *   default_database: 'default',
   *
   * these may be overwritten by each engine configurations
   */

  this._default_fetch_lines = this._conf.fetch_lines || FETCH_LINES_DEFAULT;
  this._default_query_timeout = this._conf.query_timeout || null;
  this._default_setup_queries = this._conf.setup_queries || [];
  this._default_default_database = this._conf.default_database || 'default';

  this._default_engine_label = undefined;
  this._engine_configs = {};
  this._engines = {};

  if (this._conf.executer) {
    this._default_engine_label = 'default';
    var engine_conf = {
      label: 'default',
      executer: this._conf.executer,
      monitor: this._conf.monitor
    };
    this._engine_configs['default'] = engine_conf;
    this._conf.engines = [engine_conf];
  }
  else if (this._conf.engines) {
    this._default_engine_label = this._conf.engines[0].label;

    this._conf.engines.forEach(function(e){
      if (! e.label)
        throw "label missing in engines configuration!";
      if (! e.executer)
        throw "executer not found in engines configuration!";

      self._engine_configs[e.label] = e;
    });
  }
  else
    throw "engines configuration missing";

  this.localStore(); // to initialize sqlite3 database
};

function error_callback(name, t, callback, err, data){
  console.log({name:name, err:err, data:data});
  if (! callback) return;
  if (data && data['ERROR'])
    err.message += ' ERROR:' + data['ERROR'];
  callback.apply(t, [err]);
};

Client.prototype.localStore = function(){
  if (this._localstore) {
    return this._localstore;
  }
  this._localstore = new localdiskstore.LocalDiskStore(this._conf.storage.datadir);
  return this._localstore;
};

Client.prototype.engineLabels = function(){
  var self = this;
  var labels = [];
  this._conf.engines.forEach(function(e){
    labels.push( e.label );
  });
  return labels;
};

Client.prototype.engineInfo = function(callback){
  var self = this;
  var labels = this.engineLabels();

  var response = { monitor: {} };
  var funclist = [];

  labels.forEach(function(label){
    response.monitor[label] = self.engine(label).supports('status');

    funclist.push(function(cb){
      self.databases(label, function(err,dblist){
        if (err) { cb(err); return; }
        dblists[label] = dblist;
        cb(null);
      });
    });
  });

  // default engine is head of engineLabels
  // default database is head of each database lists of engines
  // we MUST keep orders of engineLabels and database lists

  var dblists = {};

  async.parallel(funclist, function(err, results){
    if (err) { error_callback('engineInfo', self, callback, err); return; }

    var pairs = [];
    labels.forEach(function(label){
      var part = dblists[label].map(function(dbname){ return [ label, dbname ]; });
      pairs = pairs.concat(part);
    });
    response['pairs'] = pairs;

    callback.apply(self, [err, response]);
  });
};

Client.prototype.engine = function(label){
  if (!label)
    label = this._default_engine_label;

  if (this._engines[label])
    return this._engines[label];

  var conf = this._engine_configs[label];
  if (! conf)
    throw "unknown engine label:" + label;

  var executer_conf = conf.executer;
  var monitor_conf = conf.monitor;

  var options = {
    query_timeout: this._default_query_timeout,
    fetch_lines: this._default_fetch_lines,
    setup_queries: this._default_setup_queries
  };

  if (executer_conf.fetch_lines)
    options.fetch_lines = executer_conf.fetch_lines;
  if (executer_conf.query_timeout)
    options.query_timeout = executer_conf.query_timeout;
  if (executer_conf.setup_queries)
    options.setup_queries = executer_conf.setup_queries;

  if (! executer_conf.default_database)
    executer_conf.default_database = this._default_default_database;

  this._engines[label] = new engine.Engine(label, executer_conf, monitor_conf, options);
  return this._engines[label];
};

Client.prototype.end = function(){
  var client = this;
  if (this._localstore) {
    this._localstore.close();
    this._localstore = undefined;
  }
  if (this._engines) {
    for (var label in this._engines) {
      this._engines[label].close();
    }
    this._engines = [];
  }
};

var pad = function(n){return n < 10 ? '0'+n : n;};
var historyKey = function(){
  var d = new Date();
  return '' + d.getFullYear() + pad(d.getMonth() + 1);
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

Client.prototype.deleteRecent = function(queryid, callback){
  var client = this;
  this.localStore().deleteRecent(queryid, function(err){
    if (err) { error_callback('deleteRecent', client, callback, err); return; }
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

Client.prototype.createQuery = function(engineLabel, dbname, querystring, callback){
  var client = this;
  var seed;
  var query;
  try {
    seed = (new Date()).toTimeString(); // seed is not needed strictlicity
    query = new Query({querystring:querystring, engine: engineLabel, dbname: dbname, seed: seed});
  }
  catch (e) {
    error_callback('createQuery catch', client, callback, e);
    return;
  }
  client.query(query.queryid, function(err, savedquery){
    if (!err && savedquery) { callback.apply(client, [err, savedquery]); return; }
    this.localStore().insertQuery(query, function(err){
      if (err) { error_callback('createQuery', client, callback, err); return; }
      callback.apply(client, [err, query]);
    });
  });
};

/*
 * delete query, result and resultdata
 */
Client.prototype.deleteQuery = function(queryid, callback){
  var client = this;
  this.query(queryid, function(err, query){
    var deleteQuery = function(cb){
      client.localStore().deleteQuery(queryid, function(err){
        if (err) { cb(err); return; }
        client.localStore().deleteTagForQuery(queryid, function(err){
          cb(err);
        });
      });
    };
    var deleteResults = [];
    if (query) {
      deleteResults = query.results.map(function(result){ return function(cb){
        client.deleteResult(result.resultid, function(err){
          cb(err);
        });
      };});
    }

    var funcs = deleteResults.concat([deleteQuery]);
    async.parallel(funcs, function(err, results){
      callback.apply(client, [err]);
    });
  });
};

Client.prototype.tagList = function(callback){
  var client = this;
  this.localStore().tagList(function(err, tags){
    if (err) { error_callback('tagList', client, callback, err); return; }
    callback.apply(client, [err, tags]);
  });
};

Client.prototype.tags = function(queryid, callback){
  var client = this;
  this.localStore().tags(queryid, function(err, tags){
    if (err) { error_callback('tags', client, callback, err); return; }
    callback.apply(client, [err, tags]);
  });
};

Client.prototype.addTag = function(queryid, tag, callback){
  var client = this;
  this.localStore().addTag(queryid, tag, function(err){
    if (err) { error_callback('addTag', client, callback, err); return; }
    callback.apply(client, [err]);
  });
};

Client.prototype.deleteTag = function(queryid, tag, callback){
  var client = this;
  this.localStore().deleteTag(queryid, tag, function(err){
    if (err) { error_callback('deleteTag', client, callback, err); return; }
    callback.apply(client, [err]);
  });
};

Client.prototype.taggedQueries = function(tag, callback){
  var client = this;
  this.localStore().taggedQueries(tag, function(err, queryids){
    if (err) { error_callback('taggedQueries', client, callback, err); return; }
    callback.apply(client, [err, queryids]);
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

/* delete result and resultdata */
Client.prototype.deleteResult = function(resultid, callback){
  var client = this;
  this.localStore().deleteResult(resultid, function(err){
    /* ignore errors */
    client.localStore().deleteResultData(resultid, function(err){
      callback.apply(client, [null]);
    });
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

/* deleteResultData -> deleteResult */

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

Client.prototype.databases = function(engineLabel, callback){
  var client = this;
  if (! client.engine(engineLabel).supports('databases')) {
    callback.apply(client, [null, ['(default)']]);
    return;
  }
  client.engine(engineLabel).databases(function(error, data){
    callback.apply(client, [error, data]);
  });
};

Client.prototype.tables = function(engineLabel, dbname, callback){
  var client = this;
  client.engine(engineLabel).tables(dbname, function(error, data){
    callback.apply(client, [error, data]);
  });
};

/* get partitions of specified table */
Client.prototype.partitions = function(engineLabel, dbname, tablename, callback){
  var client = this;
  client.engine(engineLabel).partitions(dbname, tablename, function(error, data){
    // engine().partitions() returns
    //   [ 'part1=val1/part2=val2/part3=val3', .... ]

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

    data.forEach(function(partition){
      create_node(partition);
    });
    callback.apply(client, [error, partition_nodes]);
  });
};

/* get table schema info */
Client.prototype.describe = function(engineLabel, dbname, tablename, callback){
  var client = this;
  client.engine(engineLabel).describe(dbname, tablename, function(error, data){
    callback.apply(client, [error, data]);
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
      var executed_time = new Date();
      result = new Result({queryid:query.queryid, executed_time:executed_time});
      result.markAsExecuted({message: 'specified as "give up"'});
      client.setResult(result, function(err){
        query.results.push({executed_at:result.executed_at, resultid:result.resultid});
        client.updateQuery(query);
        if (callback)
          callback.apply(client, [null, query]);
      });
    }
  };

  if (! query.engine)
    return;

  var engine = client.engine(query.engine);
  engine.status(query.queryid, function(err, jobdata){
    if (err){ // engine doesn't support 'status' or errors with other reason
      // cannot kill with no status information
      resultMark();
      return;
    }
    if (jobdata) {// killed job terminates hive query and executer calls callback with error "killed by user"
      engine.kill(jobdata.jobid, function(err){
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

  var executed_time = new Date();
  var result = new Result({queryid:query.queryid, executed_time:executed_time});
  this.setResult(result, function(){
    query.results.push({executed_at:result.executed_at, resultid:result.resultid});
    client.updateQuery(query);
  
    if (args.prepare) args.prepare(query);

    var schemaRow = null;

    var resultLines = 0;
    var resultBytes = 0;
    var onerror = null;

    client.engine(query.engine).execute(query.queryid, query.dbname, query.composed(), {
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
        result.schema = data;
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

Client.prototype.detailStatus = function(query, callback){
  var client = this;
  this.engine(query.engine).status(query.queryid, function(err, status){
    if (err) { error_callback('detailStatus', client, callback, err); return; }
    callback.apply(client, [null, status]);
  });
};

Client.prototype.generatePath = function(key){
  return this.localStore().generatePath(key);
};
