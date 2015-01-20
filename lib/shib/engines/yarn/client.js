var http = require('http')
  , child_process = require('child_process');

var YARNClient = exports.YARNClient = function(host, port, yarn){
  this.host = host; // rm.hostname.local
  this.yarn = yarn;
};

YARNClient.prototype.status = function(jobname, callback){
  var listCommand = this.yarn + " application -list -appStates ALL | grep " + jobname;
  child_process.exec(listCommand, function(err, stdout, stderr){
    if (err) { callback(err); return; }

    var lines = stdout.toString('utf8');

    if (lines === '') { // not found
      callback(null, null);
      return;
    }

    var lineLatest = lines.split("\n")[0];
    // Application-Id Application-Name Application-Type User Queue State Final-State Progress
    var applicationId = lineLatest.split(/\s+/)[0];
    if (!applicationId || !applicationId.match(/^application_/)) {
      // something, not application id
      callback(null, null);
      return;
    }
    var statusCommand = this.yarn + " application -status " + applicationId;
    child_process.exec(statusCommand, function(err, stdout, stderr){
      if (err) { callback(err); return; }
/*
  retval['jobid'] = status['id']; // This is actually application id.
  retval['name'] = status['name'];
  retval['priority'] = "Unknown(YARN)";
  retval['state'] = status['state'];
  retval['trackingURL'] = status['trackingUrl'];
  retval['startTime'] = new Date(status['startedTime']).toLocaleString();
  retval['mapComplete'] = null;
  retval['reduceComplete'] = null;

Application Report : 
	Application-Id : application_1418723068642_21387
	Application-Name : shib-hs2-43af02eff64367ea459f7eb184c5f114
	Application-Type : MAPREDUCE
	User : hive
	Queue : default
	Start-Time : 1421733903350
	Finish-Time : 1421733966872
	Progress : 100%
	State : FINISHED
	Final-State : SUCCEEDED
	Tracking-URL : http://rm.local:8088/jobhistory/job/job_1418723068642_21387
	RPC Port : 38068
	AM Host : 4c3bf1008.livedoor
	Diagnostics : 
 */
      var status = {};
      var lines = stdout.toString('utf8').split("\n");
      lines.forEach(function(line){
        var kv = line.trim().split(/\s:\s/);
        switch (kv[0]) {
          case "Application-Id": status['jobid'] = kv[1]; break;
          case "Application-Name": status['name'] = kv[1]; break;
          case "State": status['state'] = kv[1]; break;
          case "Tracking-URL": status['trackingURL'] = kv[1]; break;
          case "Start-Time": status['startTime'] = new Date(kv[1]).toLocalString(); break;
        }
      });
      status['mapComplete'] = null;
      status['reduceComplete'] = null;

      callback(null, status);
    });
  });
};

YARNClient.prototype.kill = function(appid, callback){
  var command = this.yarn + " application -kill " + appid;
  child_process.exec(command, function(err, stdout, stderr){
    callback(err);
  });
};
