var RESTClient = require('./rest').Client;

var Monitor = exports.Monitor = function(conf) {
  if (conf.name !== 'huahin_yarn')
    throw "monitor name mismatch for huahin_yarn:" + conf.name;

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
  throw "unknown operation name (for huahin_yarn.Monitor):" + operation;
};

function convertStatus(status) { // 'status' is one of members of "app".
/*
{"apps": {"app": [
  {
    "diagnostics": "",
    "elapsedTime": "23sec",
    "finalStatus": "UNDEFINED",
    "finishTime": "",
    "id": "application_1362452667651_1406",
    "name": "'test job 1'",
    "queue": "default",
    "startTime": "Mon Mar 11 15:37:48 JST 2013",
    "state": "RUNNING",
    "trackingUI": "ApplicationMaster",
    "trackingURL": "4c3bd1118.livedoor:8088/proxy/application_1362452667651_1406/",
    "user": "hive"
  },
  {
    "diagnostics": "",
    "elapsedTime": "40sec",
    "finalStatus": "SUCCEEDED",
    "finishTime": "Sun Mar 10 05:40:43 JST 2013",
    "id": "application_1362452667651_1075",
    "name": "--- 57991d00\nSELECT count(*) as cnt, y...100(Stage-2)",
    "queue": "default",
    "startTime": "Sun Mar 10 05:40:02 JST 2013",
    "state": "FINISHED",
    "trackingUI": "History",
    "trackingURL": "4c3bd1118.livedoor:8088/proxy/application_1362452667651_1075/jobhistory/job/job_1362452667651_1075",
    "user": "hive"
  },
]}}
 */
  /*
   // hmm....
   appid, name, state, status,
   startTime, tackingUI, trackingURL,
   .....


   jobid, name, priority, state, jobSetup, status, jobCleanup,
   trackingURL, startTime, mapComplete, reduceComplete,
   hiveQueryId, hiveQueryString
   */
  //TODO rewrite!
  if (! status) {
    return null;
  }
  var data = status;
  var retval = {};
  var attrs = ['jobid','name','priority','state','jobSetup','status','jobCleanup','trackingURL','startTime','mapComplete','reduceComplete'];
  attrs.forEach(function(attr){
    retval[attr] = data[attr];
  });
  retval['hiveQueryId'] = (data['configuration'] || {})['hive.query.id'];
  retval['hiveQueryString'] = (data['configuration'] || {})['hive.query.string'];
  return retval;
};

Monitor.prototype.status = function(jobname, callback){
  var client = this._client;
  client.list(function(err, result){
    if (err) { callback(err); return; }
    if (!result) { callback(null, null); return; }
    
    var retval = null;
    result.forEach(function(app){
      if (app.name === jobname)
        retval = app;
    }); 
    callback(null, retval);
  });
};

Monitor.prototype.kill = function(jobname, callback){
  var client = this._client;
  this.status(jobname, function(err, app){
    if (!app) { callback({message:'kill target application not found:' + jobname}); return; }
    //TODO: check attribute name
    client.kill(app.id, callback);
  });
};
