var YARNClient = require('./client').YARNClient;

var Monitor = exports.Monitor = function(conf, logger) {
  if (conf.name !== 'yarn')
    throw "monitor name mismatch for yarn:" + conf.name;

  if (!conf.host || !conf.port) {
    throw "YARN WebUI host/port not specified";
  }
  var yarn = conf.yarn;
  if (! conf.yarn) {
    yarn = 'yarn';
  }
  this.logger = logger;
  this._client = new YARNClient(conf.host, conf.port, yarn);
};

Monitor.prototype.end = function(){
};

Monitor.prototype.supports = function(operation){
  switch (operation) {
  case 'status':
  case 'kill':
    return true;
  }
  throw "unknown operation name (for yarn.Monitor):" + operation;
};

Monitor.prototype.status = function(jobname, callback){
  this._client.status(jobname, callback);
};

// kill YARN application, and 'jobid' means application id
Monitor.prototype.kill = function(jobid, callback){
  this._client.kill(jobid, callback);
};
