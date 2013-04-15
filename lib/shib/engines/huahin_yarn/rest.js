var http = require('http');

var Client = exports.Client = function(host, port){
  this._host = host;
  this._port = port;
};

// curl -X GET "http://<HOSTNAME>:9010/application/list"
Client.prototype.list = function(callback){
  this.request('GET', '/application/list', callback);
};

// curl -X GET "http://<HOSTNAME>:9010/application/cluster"
Client.prototype.cluster = function(callback){
  this.request('GET', '/application/cluster', callback);
};

// curl -X DELETE "http://<HOSTNAME>:9010/application/kill/{appid}"
Client.prototype.kill = function(appid, callback){
  this.request('DELETE', '/application/kill/' + appid, callback);
};

// curl -X GET "http://<HOSTNAME>:9010/api/proxy/{appid}/ws/v1/mapreduce/info"
Client.prototype.mapreduceinfo = function(appid, callback){
  this.request('GET', '/api/proxy/' + appid + '/ws/v1/mapreduce/info', callback);
};

Client.prototype.request = function(method, path, callback){
  var options = {
    host: this._host,
    port: this._port,
    path: path,
    method: method
  };
  var cb = function(res){
    if (res.statusCode < 200 || res.statusCode >= 300) {
      callback({message: "Huahin Manager returns response code " + res.statusCode});
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
