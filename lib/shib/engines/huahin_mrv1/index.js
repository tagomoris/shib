var RESTClient = require('./rest').Client;

//TODO: Executer (after HuahinManager Hive REST API supports multi-query execution)

var Monitor = exports.Monitor = function(conf) {
  if (conf.name !== 'huahin_mrv1')
    throw "monitor name mismatch for huahin_mrv1:" + conf.name;

  this._client = new RESTClient(conf.host, conf.port);
};

Monitor.prototype.end = function(){
};

Monitor.prototype.supports = function(operation){
  switch (operation) {
  case 'status':
  case 'kill':
    return true;
  }
  throw "unknown operation name (for huahin_mrv1.Monitor):" + operation;
};

Monitor.prototype.status = function(jobname, callback){
  var client = this._client;
  client.listAll(function(err, result){
    if (err) { callback(err); return; }
    if (!result) { callback(null, null); return; }

    var jobid = null;
    result.forEach(function(job){
      if (job.name === jobname)
        jobid = jobname;
    });
    if (!jobid) { callback(null, null); return; }

    client.detail(jobid, callback);
  });
};

Monitor.prototype.kill = function(jobname, callback){
  this._client.killByName(jobname, callback);
};
