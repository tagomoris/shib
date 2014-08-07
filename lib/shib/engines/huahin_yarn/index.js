var RESTClient = require('./rest').Client;

var Monitor = exports.Monitor = function(conf, logger) {
  if (conf.name !== 'huahin_yarn')
    throw "monitor name mismatch for huahin_yarn:" + conf.name;

  this.logger = logger;
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

function convertStatus(status, info) {  // 'status' is one of members of "app", and 'info' is value of 'info'
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
   {"info": {
     "appId": "application_1363145337412_0001",
     "elapsedTime": 61245,
     "name": "select count(*) as cnt from...service='blog'(Stage-1)",
     "startedOn": 1365758130628,
     "user": "hive"
   }}
 */
  /*
  var returnedValus = {
    jobid: 'job_201304011701_1912',
    name: 'shib-3578d8d4f5a1812de7a7714f5b108776',
    priority: 'NORMAL',
    state: 'RUNNING',
    trackingURL: 'http://master.hadoop.local:50030/jobdetails.jsp?jobid=job_201304011701_1912',
    startTime: 'Thu Apr 11 2013 16:06:40 (JST)',
    mapComplete: 89,
    reduceComplete: 29,
    hiveQueryId: 'hive_20130411160606_46b1b669-3a64-4174-899e-bb1bf53e90db',
    hiveQueryString: 'SELECT ...'
  };
   */
  if (status === undefined) {
    return null;
  }
  var retval = {};

  retval['jobid'] = status['id'];
  retval['name'] = status['name'];
  retval['priority'] = 'unknown';
  retval['state'] = (status['state'] === 'FINISHED' ? status['finalStatus'] : status['state']);
  retval['trackingURL'] = status['trackingURL'];

  retval['startTime'] = (function(sourceDate){
    // convert for `new Date(string)` acceptable format
    // 'Thu Apr 11 16:06:40 JST 2013' -> 'Thu Apr 11 2013 16:06:40 (JST)'

    // [1]Weekday, [2]Month, [3]Day, [4]Time, [5]TimeZone, [6]Year
    var match = /^([A-Z][a-z]{2}) ([A-Z][a-z]{2}) (\d+) (\d\d:\d\d:\d\d) ([a-zA-Z]+) (\d+)$/.exec(sourceDate);
    if (! match) { return null; }
    // -> [1]Weekday, [2]Month, [3]Day, [6]Year, [4]Time, ([5]TimeZone)
    return [match[1], match[2], match[3], match[6], match[4], '(' + match[5] + ')'].join(' ');
  })(status['startTime']);

  retval['mapComplete'] = (status['mapComplete'] ? parseInt(status['mapComplete']) : null);
  retval['reduceComplete'] = (status['reduceComplete'] ? parseInt(status['reduceComplete']) : null);

  retval['hiveQueryId'] = (status['configuration'] || {})['hive.query.id'];
  retval['hiveQueryString'] = (status['configuration'] || {})['hive.query.string'];

  return retval;
};

Monitor.prototype.status = function(jobname, callback){
  var client = this._client;
  client.list(function(err, result){
    if (err || !result || !result['apps'] || !result['apps']['app']) { callback(err, null); return; }
    
    var appstatus = null;
    result['apps']['app'].forEach(function(app){
      if (app.name === jobname)
        appstatus = app;
    });
    if (!appstatus) { callback(null, null); return; }

    client.mapreduceinfo(appstatus['id'], function(err, info){
      if (err || !info['info']) { callback(err, null); return; }
      callback(null, convertStatus(appstatus, info['info']));
    });
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
