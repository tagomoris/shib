var http = require('http')
  , child_process = require('child_process');

var YARNClient = exports.YARNClient = function(host, port, yarn){
  this.host = host; // rm.hostname.local
  this.port = port;
  this.yarn = yarn;
};

// https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Applications_API
//
// GET "http://<rm http address:port>/ws/v1/cluster/apps"
YARNClient.prototype.applications = function(callback){
  this.request('GET', '/ws/v1/cluster/apps', callback);
};

// https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Application_API
//
// GET "http://<rm http address:port>/ws/v1/cluster/apps/{appid}"
YARNClient.prototype.application = function(appid, callback){
  this.request('GET', '/ws/v1/cluster/apps/' + appid, callback);
};

YARNClient.prototype.request = function(method, path, callback){
  var options = {
    host: this.host,
    port: this.port,
    path: path,
    method: method
  };

  var cb = function(res){
    if (res.statusCode != 200 ) {
      callback({message: "YARN REST API returns response code " + res.statusCode});
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
    });
    res.on('end', function(){
      var data = JSON.parse(jsondata);
      callback(null,data);
    }); 
  };
  var errcb = function(e){ callback(e, null); };
  http.request(options, cb).on('error', errcb).end();
};

YARNClient.prototype.status = function(jobname, callback){
  var restClient = this;
  restClient.applications(function(err, result){
    if (err || !result || !result['apps'] || !result['apps']['app']) { callback(err, null); return; }

    var appstatus = null;
    result['apps']['app'].forEach(function(app){
      if (app.name === jobname)
        appstatus = app;
    });
    if (!appstatus) { callback(null, null); return; }

    restClient.application(appstatus['id'], function(err, info){
      if (err) { callback(err, null); return; }
/*
{
   "app" : {
      "finishedTime" : 1326824991300,
      "amContainerLogs" : "http://host.domain.com:8042/node/containerlogs/container_1326821518301_0005_01_000001",
      "trackingUI" : "History",
      "state" : "FINISHED",
      "user" : "user1",
      "id" : "application_1326821518301_0005",
      "clusterId" : 1326821518301,
      "finalStatus" : "SUCCEEDED",
      "amHostHttpAddress" : "host.domain.com:8042",
      "progress" : 100,
      "name" : "shib-hs2-0ef88408da216fc6f40530c3af38f9ec",
      "applicationType" : "Yarn",
      "startedTime" : 1326824544552,
      "elapsedTime" : 446748,
      "diagnostics" : "",
      "trackingUrl" : "http://host.domain.com:8088/proxy/application_1326821518301_0005/jobhistory/job/job_1326821518301_5_5",
      "queue" : "a1",
      "memorySeconds" : 151730,
      "vcoreSeconds" : 103
   }
}
*/
      var status = {};
      status['jobid'] = info['app']['id'];
      status['name'] = info['app']['name'];
      status['state'] = info['app']['state'];
      status['trackingURL'] = info['app']['trackingUrl'];
      status['startTime'] = new Date(parseInt(info['app']['startedTime'])).toLocaleString();
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
