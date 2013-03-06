var Fetcher = function(){
  this.fetch = function(num, callback){ callback(null, []); };
};

var Executer = exports.Executer = function(){
  this.supports = function(operation){ return true; };
};

Executer.prototype.setup = function(setups, callback){
  callback(null);
};

Executer.prototype.execute = function(query, callback){
  callback(null, new Fetcher());
};

Executer.prototype.end() = function(){};

var Monitor = exports.Monitor = function(){
  this.supports = function(operation){ return false; };
};

Monitor.prototype.status = function(queryid, callback){
  callback(null, {});
};

Monitor.prototype.kill = function(queryid, callback){
  callback(null);
};

Monitor.prototype.end() = function(){};
