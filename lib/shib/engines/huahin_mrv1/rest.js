var http = require('http');

var Client = exports.Client = function(host, port){
  this.host = host;
  this.port = port;
};

Client.prototype.listAll = function(callback){
  return this.list('all', callback);
};

Client.prototype.list = function(type, callback){
  var path = '/job/list';
  switch(type) {
  case 'failed':    path = '/job/list/failed'; break;
  case 'killed':    path = '/job/list/killed'; break;
  case 'prep':      path = '/job/list/prep'; break;
  case 'running':   path = '/job/list/running'; break;
  case 'succeeded': path = '/job/list/succeeded'; break;
  }
  this.request('GET', path, callback);
};

Client.prototype.status = function(jobid, callback){
  this.request('GET', '/job/status/' + jobid, callback);
};

Client.prototype.detail = function(jobid, callback){
  this.request('GET', '/job/detail/' + jobid, callback);
};

Client.prototype.kill = function(jobid, callback){
  this.request('DELETE', '/job/kill/id/' + jobid, callback);
};

Client.prototype.killByName = function(jobname, callback){
  this.request('DELETE', '/job/kill/name/' + jobname, callback);
};

Client.prototype.request = function(method, path, callback){
  var options = {
    host: this.host,
    port: this.port,
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
