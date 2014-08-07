var url = require('url')
  , http = require('http')
  , https = require('https');

var Auth = exports.Auth = function(args, logger){
  this.logger = logger;

  var parsed = {};
  if (args.url) {
    parsed = url.parse(args.url);
    /*
{ protocol: 'http:',
  slashes: true,
  auth: null,
  host: 'localhost',
  port: null,
  hostname: 'localhost',
  hash: null,
  search: null,
  query: null,
  pathname: '/path',
  path: '/path',
  href: 'http://localhost/path' }
   */
  }

  this._url = parsed.href || args.url;
  this._method = args.method || 'GET';
  this._host = args.host || parsed.host;
  this._port = args.port || parsed.port || 80;
  this._path = args.path || '/'; // including query string

  var protocol = args.protocol || parsed.protocol || 'http';
  this._client = http;
  if (protocol === 'https' || protocol === 'https:')
    this._client = https;

  if (! this._host || ! this._path)
    throw "basic auth target host/path not speicifed";
};

Auth.prototype.check = function(username, password, callback) {
  var auth_string = username + ':' + password;
  var options = {
    host: this._host,
    port: this._port,
    method: this._method,
    path: this._path,
    auth: auth_string
  };
  var req = this._client.request(options, function(res){
    var success = (res.statusCode == 200);
    res.on('data', function(){
      callback(null, success);
    });
  });
  req.on('error', function(e){ callback(e); });
  req.end();
};

