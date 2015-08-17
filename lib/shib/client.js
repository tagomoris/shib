var Query = require('./query').Query;

var async = require('async');

var localdiskstore = require('./localdiskstore'),
    LocalDiskStoreError = localdiskstore.LocalStoreError;
var engine = require('./engine');

var FETCH_LINES_DEFAULT = 1000;

var Client = exports.Client = function(args, logger, credential){
  var self = this;

  this.logger = logger;
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

  this.auth_credential = credential;
};

function error_callback(name, t, callback, err, data){
  if (! callback) return;
  if (data && data['ERROR'])
    err.message += ' ERROR:' + data['ERROR'];
  callback.apply(t, [err]);
};

Client.prototype.localStore = function(){
  if (this._localstore) {
    return this._localstore;
  }
  this._localstore = new localdiskstore.LocalDiskStore(this._conf.storage.datadir, this.logger);
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

  // default engine is head of engineLabels
  // default database is head of each database lists of engines
  // we MUST keep orders of engineLabels and database lists

  var dblists = {};

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
    return null; // unconfigured engine label

  var executer_conf = conf.executer;
  var monitor_conf = conf.monitor;

  var options = {
    query_timeout: this._default_query_timeout,
    fetch_lines: this._default_fetch_lines,
    setup_queries: this._default_setup_queries,
    auth_credential: this.auth_credential
  };

  if (executer_conf.fetch_lines)
    options.fetch_lines = executer_conf.fetch_lines;
  if (executer_conf.query_timeout)
    options.query_timeout = executer_conf.query_timeout;
  if (executer_conf.setup_queries)
    options.setup_queries = executer_conf.setup_queries;

  if (! executer_conf.default_database)
    executer_conf.default_database = this._default_default_database;

  this._engines[label] = new engine.Engine(label, executer_conf, monitor_conf, options, this.logger);
  return this._engines[label];
};

Client.prototype.end = function(half){
  var client = this;
  if (this._localstore) {
    this._localstore.close();
    this._localstore = undefined;
  }
  if (half)
    return;
  if (this._engines) {
    for (var label in this._engines) {
      this._engines[label].close();
    }
    this._engines = undefined;
  }
};

Client.prototype.recentQueries = function(num, callback){
  var client = this;
  this.localStore().recentQueries(num, function(err, list){
    if (err) { error_callback('recentQueries', client, callback, err); return; }
    callback.apply(client, [err, list]);
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

Client.prototype.createQuery = function(engineLabel, dbname, querystring, scheduled, callback){
  var client = this;
  var seed;
  var query;
  try {
    seed = (new Date()).toTimeString(); // for queryid
    query = new Query({querystring:querystring, engine: engineLabel, dbname: dbname, scheduled: scheduled, seed: seed});
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
 * delete query, tag and resultData
 */
Client.prototype.deleteQuery = function(queryid, callback){
  var client = this;
  this.query(queryid, function(err, query){
    if (err) { callback(err); return; }
    if (query === null) { error_callback('deleteQuery', client, callback, new Error("queryid=" + queryid + " is not found in DB.")); return; }
    var resultid = query.resultid;
    async.series([
      function(cb){ client.localStore().deleteQuery(queryid, function(err){ cb(err); }); },
      function(cb){ client.localStore().deleteTagForQuery(queryid, function(err){ cb(err); }); },
      function(cb){ client.localStore().deleteResultData(resultid, function(err){ cb(err); }); }
    ], function(err, results){ callback(err); });
  });
};

Client.prototype.getQueryByResultId = function(resultid, callback){
  var client = this;
  this.localStore().queryByResultId(resultid, function(err, data){
    if (err) { error_callback('getQuery', client, callback, err, data); return; }
    callback.apply(client, [err, data]);
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

Client.prototype.databases = function(engineLabel, callback){
  var client = this;
  var engine = client.engine(engineLabel);
  if (! engine) {
    callback.apply(client, [{message: "RELOAD page: unknown engine label, " + engineLabel}]);
    return;
  }
  if (! engine.supports('databases')) {
    callback.apply(client, [null, ['(default)']]);
    return;
  }
  engine.databases(function(error, data){
    callback.apply(client, [error, data]);
  });
};

Client.prototype.tables = function(engineLabel, dbname, callback){
  var client = this;
  var engine = client.engine(engineLabel);
  if (! engine) {
    callback.apply(client, [{message: "RELOAD page: unknown engine label, " + engineLabel}]);
    return;
  }
  engine.tables(dbname, function(error, data){
    callback.apply(client, [error, data]);
  });
};

/* get partitions of specified table */
Client.prototype.partitions = function(engineLabel, dbname, tablename, callback){
  var client = this;
  var engine = client.engine(engineLabel);
  if (! engine) {
    callback.apply(client, [{message: "RELOAD page: unknown engine label, " + engineLabel}]);
    return;
  }
  engine.partitions(dbname, tablename, function(error, data){
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

    (data || []).forEach(function(partition){
      create_node(partition);
    });
    callback.apply(client, [error, partition_nodes]);
  });
};

/* get table schema info */
Client.prototype.describe = function(engineLabel, dbname, tablename, callback){
  var client = this;
  var engine = client.engine(engineLabel);
  if (! engine) {
    callback.apply(client, [{message: "RELOAD page: unknown engine label, " + engineLabel}]);
    return;
  }
  engine.describe(dbname, tablename, function(error, data){
    callback.apply(client, [error, data]);
  });
};

Client.prototype.giveup = function(query, callback){
  var client = this;

  if (! query.engine){
    client.end(); // self close after half close
    return;
  }

  var engine = client.engine(query.engine);
  if (! engine) {
    callback.apply(client, [{message: "RELOAD page: unknown engine label, " + query.engine}]);
    client.end(); // self close after half close
    return;
  }

  engine.status(query.queryid, function(err, jobdata){
    if (err){ // engine doesn't support 'status' or errors with other reason
      // cannot kill with no status information
      error_callback('giveup', client, callback, err)
      client.end(); // self close after half close
      return;
    }
    if (jobdata) {// killed job terminates hive query and executer calls callback with error "killed by user"
      engine.kill(jobdata.jobid, function(err){
        if (err) {
          client.logger.error("Error on killing job", {jobid: jobdata.jobid, error: err});
        }

        query.markAsExecuted({message: 'specified as "give up"'});
        client.updateQuery(query);
        if (callback)
          callback.apply(client, [null, query]);

        client.end(); // self close after half close
      });
    }
  });
};

Client.prototype.execute = function(query, args){
  if (! args)
    args = {};

  var client = this;

  var executed_time = new Date();
  //query.state is already running, and resultid/result is set
  
  if (args.prepare)
    args.prepare(query);

  var schemaRow = null;

  var resultLines = 0;
  var resultBytes = 0;
  var onerror = null;

  var engine = client.engine(query.engine);
  if (! engine) {
    query.markAsExecuted({message: "RELOAD page: unknown engine label, " + query.engine});
    if (args.error)
      args.error();
    return;
  }
  engine.execute(query.queryid, query.dbname, query.composed(), {
    stopcheck: args.stopCheck,
    stop: args.stop,
    complete: function(err){
      query.markAsExecuted(onerror, resultLines, resultBytes);
      client.updateQuery(query);

      if (onerror && args.error)
        args.error();
      else if (args.success)
        args.success();
    },
    error: function(err){
      query.markAsExecuted(err);
      client.updateQuery(query);
      if (args.error)
        args.error();
    },
    timeout: function(err){
      var queryid = query.queryid;
      query.markAsExecuted(err);
      client.updateQuery(query, function(err){ client.end(true); }); // early half close to close database
      // DO NOT operate database after here
      if (engine.supports('status') && engine.supports('kill')) {
        engine.status(queryid, function(err, jobdata){
          if (err){
            if (args.error)
              args.error();
            return;
          }
          if (jobdata) {// killed job terminates hive query and executer calls callback with error "killed by user"
            engine.kill(jobdata.jobid, function(err){
              if (err) { client.logger.error("Error on killing job", {jobid: jobdata.jobid, error: err}); }
              if (args.error)
                args.error();
            });
          }
        });
      }
      else {
        if (args.error)
          args.error();
      }
    },
    schema: function(error, data){
      if (error){ onerror = error; return; }
      query.addSchema(data);
      // set to write to top of result data
      schemaRow = data.map(function(f){return f.name.toUpperCase();}).join('\t');
    },
    fetch: function(error, rows, cb){
      if (error){ onerror = error; cb(); return; }
      if (schemaRow) {
        rows.unshift(schemaRow);
        schemaRow = null;
      }
      client.appendResultData(query.resultid, rows, function(err){
        if (err) {
          client.logger.error('failed to append result data', err);
          cb(err);
          throw new LocalDiskStoreError("failed to append result data");
        }
        resultLines += rows.length;
        resultBytes += rows.reduce(function(prev,v){return prev + v.length + 1;}, 0);
        cb();
      });
    }
  });
};

Client.prototype.detailStatus = function(query, callback){
  var client = this;
  var engine = client.engine(query.engine);
  if (! engine) {
    error_callback('detailStatus', client, callback, {message: "RELOAD page: unknown engine label, " + query.engine}); return;
    return;
  }
  engine.status(query.queryid, function(err, status){
    if (err) { error_callback('detailStatus', client, callback, err); return; }
    callback.apply(client, [null, status]);
  });
};

Client.prototype.generatePath = function(key){
  return this.localStore().generatePath(key);
};
