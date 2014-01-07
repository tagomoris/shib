var Fetcher = function(){
  this.fetch = function(num, callback){ callback(null, []); };
};

var Executer = exports.Executer = function(){
  this.supports = function(operation){ return true; };
};

Executer.prototype.setup = function(setups, callback){
  callback(null);
};

Executer.prototype.execute = function(jobname, query, callback){ //TODO: jobname -> queryid ( jobname generation should be done in each engines)
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

Monitor.prototype.status = function(queryid, callback){
  callback(null, {});
};

Monitor.prototype.kill = function(queryid, callback){
  callback(null);
};

Monitor.prototype.end = function(){};
