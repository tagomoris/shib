var http = require('http')
  , child_process = require('child_process');

var YARNClient = exports.YARNClient = function(host, port, yarn){
  this.host = host; // jobtracker.hostname.local
  this.port = port; // 8088
  this.yarn = yarn;
};

YARNClient.prototype.listAll = function(callback){
  /*
  var example = {
    "apps": {
      "app": [
        {
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
        }
      ]
    }
  };
   */

  this.request('/ws/v1/cluster/apps', function(err, data){
    if (err) { callback(err); return; }
    callback(null, data['apps']['app']);
  });
};

YARNClient.prototype.kill = function(appid, callback){
  var command = this.yarn + " application -kill " + appid;
  child_process.exec(command, function(err, stdout, stderr){
    callback(err);
  });
};

YARNClient.prototype.request = function(path, callback){
  var options = {
    host: this.host,
    port: this.port,
    path: path,
    method: 'GET'
  };
  var cb = function(res){
    if (res.statusCode < 200 || res.statusCode >= 300) {
      callback({message: "YARN API returns response code " + res.statusCode});
      return; 
    }
    // status: 2xx

    if (! res.headers['content-type'].match(/^application\/json/)) {
      callback(null);
      return;
    }

    // content-type: application/json
    var jsondata = '';
    res.on('data', function(chunk){
      jsondata += chunk;
      var data = null;
      try {
        data = JSON.parse(jsondata);
      }
      catch (e) { /* jsondata is not complete */
        data = null;
      }
      if (data) {
        callback(null,data);
        jsondata = null;
      }
    });
  };
  var errcb = function(e){ callback(e, null); };
  http.request(options, cb).on('error', errcb).end();
};
