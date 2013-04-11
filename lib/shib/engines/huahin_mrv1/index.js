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

function convertStatus(status) {
  /* TODO: these values will be thrown away?
{
  "groups": {
    "FileSystemCounters": {
      "FILE_BYTES_WRITTEN": 12654144,
      "HDFS_BYTES_READ": 1395562717,
      "HDFS_BYTES_WRITTEN": 212242870
    },
    "Job Counters ": {
      "Data-local map tasks": 166,
      "Failed map tasks": 1,
      "Launched map tasks": 176,
      "Rack-local map tasks": 10,
      "SLOTS_MILLIS_MAPS": 945952,
      "SLOTS_MILLIS_REDUCES": 0,
      "Total time spent by all maps waiting after reserving slots (ms)": 0,
      "Total time spent by all reduces waiting after reserving slots (ms)": 0
    },
    "Map-Reduce Framework": {
      "Map input bytes": 1395511589,
      "Map input records": 4061894,
      "Map output records": 0,
      "SPLIT_RAW_BYTES": 51128,
      "Spilled Records": 0
    },
    "org.apache.hadoop.hive.ql.exec.FileSinkOperator$TableIdEnum": {"TABLE_ID_1_ROWCOUNT": 4061894},
    "org.apache.hadoop.hive.ql.exec.FilterOperator$Counter": {
      "FILTERED": 0,
      "PASSED": 4061894
    },
    "org.apache.hadoop.hive.ql.exec.MapOperator$Counter": {"DESERIALIZE_ERRORS": 0},
    "org.apache.hadoop.hive.ql.exec.Operator$ProgressCounter": {"CREATED_FILES": 166}
  },
}
*/
  /*
   jobid, name, priority, state, jobSetup, status, jobCleanup,
   trackingURL, startTime, mapComplete, reduceComplete,
   hiveQueryId, hiveQueryString
   */
  if (status === undefined || status['data'] === undefined) {
    return null;
  }
  var data = status.data;
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
  client.listAll(function(err, result){
    if (err || !result) { callback(err, null); return; }

    var jobid = null;
    result.forEach(function(job){
      if (job.name === jobname)
        jobid = jobname;
    });
    if (!jobid) { callback(null, null); return; }

    client.detail(jobid, function(err, data){
      if (err) { callback(err); return; }
      callback(null, convertStatus(data));
    });
  });
};

Monitor.prototype.kill = function(jobname, callback){
  this._client.killByName(jobname, callback);
};
