var Fetcher = function(){
  this.fetch = function(num, callback){ callback(null, []); };
};

var Executer = exports.Executer = function(){
  this.supports = function(operation){ return true; };
};

Executer.prototype.jobname = function(queryid) {
  return 'dummy-shib-' + queryid;
};

Executer.prototype.setup = function(setups, callback){
  callback(null);
};

Executer.prototype.databases = function(callback){
  callback(null, []); // ['db1', 'db2']
};

Executer.prototype.tables = function(dbname, callback){
  callback(null, []); // ['table1', 'table2']
};

Executer.prototype.partitions = function(dbname, tablename, callback){
  callback(null, []); // ['f1=va1/f2=vb1', 'f1=va1/f2=vb2']
};

Executer.prototype.describe = function(dbname, tablename, callback){
  callback(null, []); // [ [ 'fieldname', 'type', 'comment' ], ... ]
};

Executer.prototype.execute = function(jobname, dbname, query, callback){
  callback(null, new Fetcher());
};

/*
 * Fetcher
 *
 * schema(callback): callback(err, schema)
 *  schema: {fieldSchemas: ['fieldname1', 'fieldname2', 'fieldname3', ...]}
 *  //?? schema: ['fieldname1', 'fieldname2', ...]
 *
 * fetch(num, callback): callback(err, rows, cb)
 *  num: rows to fetch (null == all)
 *  rows: ["_col1value_\t_col2value_\t_col3value_", "_col1value_\t_col2value_\t_col3value_", ...]
 *    no more rows exists if (rows === null || rows.length < 1 || (rows.length == 1 && rows[0].length < 1))
 *  cb: callback function to call after iterative callback process
 */

Executer.prototype.end = function(){};

var Monitor = exports.Monitor = function(){
  this.supports = function(operation){ return false; };
};

Monitor.prototype.status = function(jobname, callback){
  callback(null, {});
};

Monitor.prototype.kill = function(jobid, callback){
  callback(null);
};

Monitor.prototype.end = function(){};
