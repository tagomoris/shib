var dummyengine = require('./engines/dummyengine')
  , dummy = require('./engines/dummy')
  , hiveserver = require('./engines/hiveserver')
  , hiveserver2 = require('./engines/hiveserver2')
  , presto = require('./engines/presto')
  , jobtracker = require('./engines/jobtracker')
  , yarn = require('./engines/yarn')
  , huahin_mrv1 = require('./engines/huahin_mrv1')
  , bigquery = require('./engines/bigquery');

var Query = require('./query').Query
  , AccessControl = require('./access_control').AccessControl;

var Engine = exports.Engine = function(label, executer_conf, monitor_conf, options, logger) {
  // query_timeout is for timeout between 'setup' and just after 'execute' callback call.

  this.label = label;
  this.logger = logger;

  // this._executer = dummyengine.Executer();
  this._query_timeout = options.query_timeout;
  this._setup_queries = options.setup_queries;
  this._setup_queries_auth = options.setup_queries_auth;
  this._fetch_lines = options.fetch_lines;
  this._auth_credential = options.auth_credential;
  this._executer = null;
  this._monitor = null;

  if (! executer_conf || !executer_conf.name) {
    throw "executer configuration or executer name not found";
  }

  var executer = null;
  var monitor = null;

  switch (executer_conf.name) {
  case 'hiveserver':
    executer = hiveserver.Executer;
    break;
  case 'hiveserver2':
    executer = hiveserver2.Executer;
    // monitor = hiveserver2.Monitor;
    break;
  case 'presto':
    executer = presto.Executer;
    monitor = presto.Monitor; // default monitor
    break;
  case 'bigquery':
    executer = bigquery.Executer;
    monitor = bigquery.Monitor; // default monitor
    break;
  case 'dummy':
    executer = dummy.Executer;
    monitor = dummy.Monitor;
    break;
  default:
    throw "unknown executer name:" + executer_conf.name;
  }

  if (! executer_conf.default_database)
    throw "BUG: default_database missing in executer_conf";
  this._default_dbname = executer_conf.default_database;

  this._executer = new executer(executer_conf, this.logger);

  this._acl_config = executer_conf.access_control || {};

  if (monitor_conf) {
    if (! monitor_conf.name)
      throw "monitor name not found";

    switch (monitor_conf.name) {
    case 'jobtracker':
      monitor = jobtracker.Monitor;
      break;
    case 'yarn':
      monitor = yarn.Monitor;
      break;
    case 'huahin_mrv1':
      monitor = huahin_mrv1.Monitor;
      break;
    case 'presto':
      monitor = presto.Monitor;
      break;
    case 'bigquery':
      monitor = bigquery.Monitor;
      break;
    case 'dummy':
      monitor = dummy.Monitor;
      break;
    default:
      throw "unknown monitor name:" + monitor_conf.name;
    }

    this._monitor = new monitor(monitor_conf, this.logger);
  } else if (monitor) {
    // default monitor same with executor
    this._monitor = new monitor(executer_conf, this.logger);
  } else {
    this._monitor = new dummyengine.Monitor();
  }
};

Engine.prototype.supports = function(operation) {
  switch (operation) {
    case 'jobname':
    case 'setup':
    case 'databases':
    case 'tables':
    case 'partitions':
    case 'describe':
    case 'execute':
    return this._executer.supports(operation); // executer MUST support setup() and execute()
    case 'status':
    case 'kill':
    return this._monitor.supports(operation); // monitor CAN support status() and kill()
  }
  throw "invalid operation for engines:" + operation;
};

Engine.prototype.acl = function(){
  if (this._acl_config['delegate'] === 'auth') {
    if (! this._auth_credential) {
      this.logger.warn("auth credential not found: default denied for all requests");
      return AccessControl.defaultDenyDelegator();
    }
    return this._auth_credential;
  }
  return new AccessControl(this._acl_config);
};

Engine.prototype.close = function(){
  this._executer.end();
  this._executer = undefined;
  this._monitor.end();
  this._monitor = undefined;
};

Engine.prototype.default_database_name = function(){
  if (this.supports('database'))
    return this._default_dbname;
  return null;
};

Engine.prototype.databases = function(callback){
  var self = this;
  if (this.supports('databases')) {
    this._executer.databases(function(err, dbnamelist){
      if (err) { self.logger.error('databases', err); callback(err); return; }

      var dbs = [self._default_dbname];

      dbnamelist.forEach(function(dbname){
        if (dbname === self._default_dbname)
          return;
        if (self.acl().visible(dbname))
          dbs.push(dbname);
      });

      callback(null, dbs);
    });
  } else {
    callback({message:'Failed to get database list, not supported'});
  }
};

Engine.prototype.tables = function(dbname, callback){
  var self = this;
  if (this.supports('tables')) {
    if (! this.acl().visible(dbname)) {
      callback(null, []);
      return;
    }
    this._executer.tables(dbname, function(err, data){
      var tables = [];
      data.forEach(function(tablename){
        if (self.acl().allowed(tablename, dbname))
          tables.push(tablename);
      });
      callback(null, tables);
    });
  } else {
    callback({message:'Failed to get table list, not supported'});
  }
};

Engine.prototype.partitions = function(dbname, tablename, callback){
  if (this.supports('partitions')) {
    if (! this.acl().allowed(tablename, dbname)) {
      callback({message:'access denied'});
      return;
    }
    this._executer.partitions(dbname, tablename, callback);
  } else {
    callback({message:'Failed to get partition list, not supported'});
  }
};

Engine.prototype.describe = function(dbname, tablename, callback){
  if (this.supports('describe')) {
    if (! this.acl().allowed(tablename, dbname)) {
      callback({message:'access denied'});
      return;
    }
    this._executer.describe(dbname, tablename, callback);
  } else {
    callback({message:'Failed to get table schema, not supported'});
  }
};

/*
options: {
  schema: function(error, data){},
  callback: function(error, rows){}, // default callback (exclusive with error/success/fetch/complete)
  stopcheck: function(query){},
  stop: function(){},
  error: function(error){},
  fetch: function(error, rows){},
  complete: function(error){},
  success: function(){} // without fetchNum, fetchAll() -> success_callback(result_rows) (or callback(null,result_rows) )
}
 */
Engine.prototype.execute = function(queryid, dbname, query, auth, options) {
  var self = this;

  var executer = this._executer;

  if (options.call) { // 3rd argument is single callback function
    var callback = options;
    options = {callback: callback};
  }

  var schema_callback = options.schema || function(err,data){};

  var stopcheck = options.stopcheck || function(){return false;};
  var success_callback = options.success || options.callback || function(err,data){};

  var stop_callback = options.stop || options.callback || function(err,data){};

  var error_callback = options.error || options.callback || function(err){};
  // query_timeout is for timeout between 'setup' and just after 'execute' callback call.
  var timeout_callback = options.timeout || error_callback || function(err){};

  var setups = this._setup_queries || [];
  if(auth) {
    setups = setups.concat(this._setup_queries_auth)
  }
  var fetchnum = this._fetch_lines;

  var fetch_callback = null;
  var complete_callback = null;
  if (fetchnum) {
    if (!options.fetch || !options.complete)
      throw "missing fetch or complete callback for fetch api";
    fetch_callback = options.fetch;
    complete_callback = options.complete;
  }

  var timeout_watch = null;
  var disable_timeout_handler = function(){
    if(timeout_watch && timeout_watch.state) {
      timeout_watch.state = false;
      clearTimeout(timeout_watch.timer);
    }
  };
  if (this._query_timeout) {
    var timeout_seconds = this._query_timeout * 1000;
    var timer = setTimeout(function(){
      if (timeout_watch && timeout_watch.state) {
        timeout_watch.expired = true;
        timeout_callback({message: 'query is expired with configured timeout seconds (' + String(timeout_seconds) + ')'});
      }
    }, timeout_seconds);
    var state = true; // not expired
    timeout_watch = { timer:timer, state:state, expired:false };
  }

  var table_db_pairs = Query.parseTableNames(query);
  var access_denied = false;
  table_db_pairs.forEach(function(pair){
    if (! self.acl().allowed(pair[0], (pair[1] || dbname)))
      access_denied = true;
  });

  if (access_denied) {
    disable_timeout_handler();
    error_callback({message: "access denied for db/table"});
    return;
  }

  executer.setup(setups, function(err){
    var jobname = executer.jobname(queryid);
    executer.execute(jobname, dbname, query, function(err, fetcher){
      if (timeout_watch && timeout_watch.expired) {
        // this query has expired, and timeout_callback() has already been executed
        return;
      }
      if (stopcheck()) { disable_timeout_handler(); stop_callback({message: "stopped by stopcheck"}); return; }
      if (err) {         disable_timeout_handler(); error_callback(err); return; }

      fetcher.schema(function(err, data){
        if (err) { disable_timeout_handler(); error_callback(err); return; }

        if (timeout_watch && timeout_watch.expired) {
          // this query has expired, and timeout_callback() has already been executed
          return;
        }

        disable_timeout_handler();

        if (schema_callback)
          schema_callback(err, data);

        // fetch all result rows at once
        if (! fetchnum) {
          fetcher.fetch(null, function(err, rows){ // fetch all records
            if (err) { error_callback(err); return; }
            success_callback(null, rows);
          });
          return;
        }

        var has_errors = false;
        // fetch N-rows step by step
        var continuous_fetch = function(err){
          if (err) {
            has_errors = true;
            self.logger.warn("fetch killed with upper layer error", {err: err});
            complete_callback({message:"Fetching exits with errors. " + err.message});
            return;
          }

          fetcher.fetch(fetchnum, function(err, rows){
            if (err) {
              has_errors = true;
              self.logger.warn("fetch error", {err: err});
            }

            if (stopcheck()) { stop_callback({message: "stopped by stopcheck"}); return; }

            if (rows == null || rows.length < 1 || (rows.length == 1 && rows[0].length < 1)) {
              // end of fetched rows
              if (has_errors)
                complete_callback({message:"HiveQL exits with errors"});
              else
                complete_callback(null);
              return;
            }
            fetch_callback(err, rows, continuous_fetch);
          });
        };
        continuous_fetch(null);
      });
    });
  });
};

Engine.prototype.status = function(queryid, callback) {
  var jobname = this._executer.jobname(queryid);
  if (this.supports('status')) {
    this._monitor.status(jobname, callback);
  } else {
    callback({message:'Failed to get job(' + jobname + ') status, not supported'});
  }
};

Engine.prototype.kill = function(jobid, callback) {
  if (this.supports('kill')) {
    this._monitor.kill(jobid, callback);
  } else {
    callback({message:'Failed to kill job(jobid:' + jobid + '), not supported'});
  }
};
