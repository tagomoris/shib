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

function convertStatus(status) {
  if (status === undefined) {
    return null;
  }
  var retval = {};

  /*
  var status = {
    "id": "application_1403766586195_0850",
    "user": "hive",
    "name": "shib-hs2-778ad964bf9b514422bbfeaf9f50fb48",
    "queue": "default",
    "state": "FINISHED",
    "finalStatus": "SUCCEEDED",
    "progress": 100,
    "trackingUI": "History",
    "trackingUrl": "http://LCBIPEX1504.nhnjp.ism:20888/proxy/application_1403766586195_0850/jobhistory/job/job_1403766586195_0850",
    "diagnostics": "",
    "clusterId": 1403766586195,
    "applicationType": "MAPREDUCE",
    "applicationTags": "",
    "startedTime": 1404439222862,
    "finishedTime": 1404439240204,
    "elapsedTime": 17342,
    "amContainerLogs": "http://LCBIPEX1512.nhnjp.ism:8042/node/containerlogs/container_1403766586195_0850_01_000001/hive",
    "amHostHttpAddress": "LCBIPEX1512.nhnjp.ism:8042",
    "allocatedMB": 0,
    "allocatedVCores": 0,
    "runningContainers": 0
  };
   */

  retval['jobid'] = status['id']; // This is actually application id.
  retval['name'] = status['name'];
  retval['priority'] = "Unknown(YARN)";
  retval['state'] = status['state'];
  retval['trackingURL'] = status['trackingUrl'];

  retval['startTime'] = new Date(status['startedTime']).toLocaleString();

  retval['mapComplete'] = null;
  retval['reduceComplete'] = null;

  return retval;
};

Monitor.prototype.status = function(jobname, callback){
  var client = this._client;
  client.listAll(function(err, result){
    if (err || !result) { callback(err, null); return; }

    var application = null;
    result.forEach(function(app){
      if (app.name === jobname) {
        application = app;
      }
    });

    //TODO: track application (appmaster) -> job
    // http://hadoop.apache.org/docs/r2.3.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
    // http://hadoop.apache.org/docs/r2.3.0/hadoop-yarn/hadoop-yarn-site/NodeManagerRest.html
    // http://hadoop.apache.org/docs/r2.3.0/hadoop-yarn/hadoop-yarn-site/MapredAppMasterRest.html

    if (!application) { callback(null, null); return; }
    callback(null, convertStatus(application));
  });
};

// kill YARN application, and 'jobid' means application id
Monitor.prototype.kill = function(jobid, callback){
  this._client.kill(jobid, callback);
};
