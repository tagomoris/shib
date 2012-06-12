var http = require('http');

var HuahinClient = exports.HuahinClient = function(host, port){
  this.host = host;
  this.port = port;
};

HuahinClient.prototype.listAll = function(callback){
  return this.list('all', callback);
};

HuahinClient.prototype.list = function(type, callback){
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

HuahinClient.prototype.status = function(jobid, callback){
  this.request('GET', '/job/status/' + jobid, callback);
};

HuahinClient.prototype.detail = function(jobid, callback){
  this.request('GET', '/job/detail/' + jobid, callback);
};

HuahinClient.prototype.kill = function(jobid, callback){
  this.request('DELETE', '/job/kill/id/' + jobid, callback);
};

HuahinClient.prototype.killByName = function(jobname, callback){
  this.request('DELETE', '/job/kill/name/' + jobname, callback);
};

HuahinClient.prototype.request = function(method, path, callback){
  var options = {
    host: this.host,
    port: this.port,
    path: path,
    method: method
  };
  var cb = function(res){
    if (res.statusCode >= 200 && res.statusCode < 300) {
      if (res.headers['content-type'].match(/^application\/json/)) {
        var jsondata = '';
        res.on('data', function(chunk){
          jsondata += chunk;
          var data = null;
          try {
            data = JSON.parse(jsondata);
          }
          catch (e) {
            /* jsondata is not complete */
            data = null;
          }
          if (data) {
            callback(null, {success: true, data: data});
            jsondata = null;
          }
        });
        return;
      }
      callback(null, {success: true});
      return;
    }
    callback({message: "Huahin Manager returns response code " + res.statusCode});
  };
  var errcb = function(e){ callback(e, null); };
  http.request(options, cb).on('error', errcb).end();
};
