var dummyengine = require('./engines/dummyengine'),
    hiveserver = require('./engines/hiveserver'),
    /* hiveserver2 = require('./engines/hiveservers'), */
    /* huahin_yarn = require('./engines/huahin_yarn'),*/
    huahin_mrv1 = require('./engines/huahin_mrv1');

var Engine = exports.Engine = function(executer_conf, monitor_conf, query_timeout) {
  // query_timeout is for timeout between 'setup' and just after 'execute' callback call.

  // this._executer = dummyengine.Executer();
  this._query_timeout = query_timeout;
  this._executer = null;
  this._monitor = null;

  if (! executer_conf || !executer_conf.name) {
    throw "executer configuration or executer name not found";
  }

  var executer = null;

  switch (executer_conf.name) {
  case 'hiveserver':
    executer = hiveserver.Executer;
    break;

  default:
    throw "unknown executer name:" + executer_conf.name;
  }

  this._executer = new executer(executer_conf);

  if (monitor_conf) {
    if (! monitor_conf.name)
      throw "monitor name not found";

    var monitor = null;

    switch (monitor_conf.name) {
    case 'huahin_mrv1':
      monitor = huahin_mrv1.Monitor;
      break;

      /*
    case 'huahin_yarn':
      monitor = huahin_yarn.Monitor;
      break;
       */

    default:
      throw "unknown monitor name:" + monitor_conf.name;
    }

    this._monitor = new monitor(monitor_conf);
  } else {
    this._monitor = new dummyengine.Monitor();
  }
};

Engine.prototype.supports = function(operation) {
  switch (operation) {
    case 'setup':
    case 'execute':
    return this._executer.supports(operation); // executer MUST support setup() and execute()
    case 'status':
    case 'kill':
    return this._monitor.supports(operation); // monitor CAN support status() and kill()
  }
  throw "invalid operation for engines:" + operation;
};

Engine.prototype.close = function(){
  this._executer.end();
  this._executer = undefined;
  this._monitor.end();
  this._monitor = undefined;
};

/*
options: {
  schema: function(error, data){},
  setups: [],
  fetchNum: 10, // default null
  callback: function(error, rows){}, // default callback (exclusive with error/success/fetch/complete)
  stopcheck: function(query){},
  stop: function(){},
  error: function(error){},
  fetch: function(error, rows){},
  complete: function(error){},
  success: function(){} // without fetchNum, fetchAll() -> success_callback(result_rows) (or callback(null,result_rows) )
}
 */
Engine.prototype.execute = function(jobname, query, options) {
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

  var fetchnum = null;
  var fetch_callback = null;
  var complete_callback = null;
  if (options.fetchNum) {
    if (!options.fetch || !options.complete)
      throw "missing fetch or complete callback for fetch api";
    fetchnum = options.fetchNum;
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

  executer.setup(options.setups || [], function(err){
    executer.execute(jobname, query, function(err, fetcher){
      if (timeout_watch && timeout_watch.expired) {
        // this query has expired, and timeout_callback() has already been executed
        return;
      }
      if (stopcheck()) { disable_timeout_handler(); stop_callback({message: "stopped by stopcheck"}); return; }
      if (err) {         disable_timeout_handler(); error_callback(err); return; }

      fetcher.schema(function(err, data){
        if (timeout_watch && timeout_watch.expired) {
          // this query has expired, and timeout_callback() has already been executed
          return;
        }

        disable_timeout_handler();

        if (schema_callback)
          schema_callback(err, data);
        
        // fetch all result rows at once
        if (fetchnum === null) {
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
            console.log({message:"fetch killed with upper layer error.", err:err});
            complete_callback({message:"Fetching exits with errors"});
            return;
          }

          fetcher.fetch(fetchnum, function(err, rows){
            if (stopcheck()) { stop_callback({message: "stopped by stopcheck"}); return; }

            if (rows === null || rows.length < 1 || (rows.length == 1 && rows[0].length < 1)) {
              // end of fetched rows
              if (has_errors)
                complete_callback({message:"HiveQL exits with errors"});
              else
                complete_callback(null);
              return;
            }
            if (err)
              has_errors = true;
            fetch_callback(err, rows, continuous_fetch);
          });
        };
        continuous_fetch(null);
      });
    });
  });
};

Engine.prototype.status = function(jobname, callback) {
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
