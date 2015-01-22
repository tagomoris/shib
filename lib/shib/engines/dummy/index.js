/*
 * This engine is for development on local environment
 */

var Executer = exports.Executer = function(conf, logger){
  if (conf.name !== 'dummy')
    throw "executer name mismatch for dummy:" + conf.name;

  this.logger = logger;
};

Executer.prototype.end = function(){
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
  throw "unknown operation name (for dummy):" + operation;
};

Executer.prototype.jobname = function(queryid) {
  return 'dummy-' + queryid;
};

Executer.prototype.setup = function(setups, callback){
  callback(null);
};

Executer.prototype.databases = function(callback){
  callback(null, [ ['default'], ['dummy1'] ]);
};

Executer.prototype.tables = function(dbname, callback){
  callback(null, [ ['t1'], ['t2'] ]);
};

Executer.prototype.partitions = function(dbname, tablename, callback){
  callback(null, ['f1=1/f2=1', 'f1=1/f2=2', 'f1=2/f2=1', 'f1=2/f2=2']);
};

Executer.prototype.describe = function(dbname, tablename, callback){
  callback(null, [ ['f1', 'string', ''], ['f2', 'string', ''], ['id', 'bigint', ''], ['json', 'string', ''] ]);
};

Executer.prototype.execute = function(jobname, dbname, query, callback){
  callback(null, new Fetcher(this));
};

/*
 * Fetcher
 *
 * schema(callback): callback(err, schema)
 *  schema: [ { name: 'fieldname1', type: 'typename' }, ...]
 *
 * fetch(num, callback): callback(err, rows)
 *  num: rows to fetch (null == all)
 *  rows: ["_col1value_\t_col2value_\t_col3value_", "_col1value_\t_col2value_\t_col3value_", ...]
 *    no more rows exists if (rows === null || rows.length < 1 || (rows.length == 1 && rows[0].length < 1))
 */

var Fetcher = function(client){
  this.schema = function(callback){
    callback(null, [ {name:"f1", type:"string"}, {name:"f2", type:"string"}, {name:"id", type:"bigint"}, {name:"json", type:"string"} ]);
  };

  var finished = false;
  this.fetch = function(num, callback){
    // always returns only 4 row after 10 seconds
    if (finished) {
      callback(null, []);
      return;
    }
    setTimeout(function(){
      callback(null, ["1\t1\t1000\t{}", "1\t2\t1001\t{}", "2\t1\t1002\t{}", "2\t2\t1003\t{}"]);
    }, 10000);
  };
};

var Monitor = exports.Monitor = function(conf){
  if (conf.name !== 'dummy')
    throw "executer name mismatch for dummy:" + conf.name;
};

Monitor.prototype.end = function(){
};

Monitor.prototype.supports = function(operation){
  switch (operation) { // "monitor" methods
  case 'status':
  case 'kill':
    return true;
  }
  throw "unknown operation name (for dummy.Monitor):" + operation;
};

Monitor.prototype.status = function(jobname, callback){
  callback(null, {
    jobid: "dummy-id-000001",
    name: jobname,
    priority: "unknown",
    state: "RUNNING",
    trackingURL: "http://localhost/dummy/",
    startTime: new Date().toLocaleString(),
    mapComplete: null,
    reduceComplete: null
  });
};

Monitor.prototype.kill = function(query_id, callback){
  callback(null);
};
