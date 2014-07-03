var MRv1Client = require('./client').MRv1Client;

var Monitor = exports.Monitor = function(conf) {
  if (conf.name !== 'jobtracker')
    throw "monitor name mismatch for jobtracker:" + conf.name;

  if (!conf.host || !conf.port) {
    throw "JobTracker WebUI host/port not specified";
  }
  var mapred = conf.mapred;
  if (! conf.mapred) {
    mapred = 'mapred';
  }

  this._client = new MRv1Client(conf.host, conf.port, mapred);
};

Monitor.prototype.end = function(){
};

Monitor.prototype.supports = function(operation){
  switch (operation) {
  case 'status':
  case 'kill':
    return true;
  }
  throw "unknown operation name (for jobtracker.Monitor):" + operation;
};

function convertStatus(status) {
  if (status === undefined) {
    return null;
  }
  var retval = {};

  retval['jobid'] = status['jobid'];
  retval['name'] = status['name'];
  retval['priority'] = status['priority'];
  retval['state'] = status['state'];
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

  return retval;
};

Monitor.prototype.status = function(jobname, callback){
  var client = this._client;
  client.listAll(function(err, result){
    if (err || !result) { callback(err, null); return; }

    var jobid = null;
    var opts = {
    };
    result.forEach(function(job){
      if (job.name === jobname) {
        jobid = job.jobid;
        opts['priority'] = job.priority;
      }
    });
    if (!jobid) { callback(null, null); return; }

    client.detail(jobid, opts, function(err, data){
      if (err) { callback(err); return; }
      callback(null, convertStatus(data));
    });
  });
};

Monitor.prototype.kill = function(jobid, callback){
  this._client.kill(jobid, callback);
};
