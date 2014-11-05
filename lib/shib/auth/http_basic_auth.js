var url = require('url')
  , http = require('http')
  , https = require('https');

var AccessControl = require('shib/access_control').AccessControl;

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

  this._acl_config = args.access_control;
};

Auth.prototype.check = function(req, callback) {
  var username = req.body.username;
  var password = req.body.password;

  if (!username || !password) {
    callback(null, false);
    return;
  }

  var auth_string = username + ':' + password;
  var options = {
    host: this._host,
    port: this._port,
    method: this._method,
    path: this._path,
    auth: auth_string
  };
  var authreq = this._client.request(options, function(res){
    var success = false;
    if (res.statusCode == 200) {
      success = {username: username, password: password};
    }
    var waiting = true;
    res.on('data', function(){
      if (waiting) {
        waiting = false;
        callback(null, success);
      }
    });
  });
  authreq.on('error', function(e){ callback(e); });
  authreq.end();
};

/*
var auth = {
  type: 'http_basic_auth',
  url: '....',
  realm: '',
  access_control: {
    users: {
      normal_user_name: {
        databases: {
          public: { default: "allow" }
        },
        default: "deny"
      },
      super_user_name: {
        default: "allow"
      }
    },
    default: "allow" // default
  },
};
*/

Auth.prototype.acl_delegators = function(req, username, options) {
  var acl_config = null;
  if (username && this._acl_config && this._acl_config['users'] && this._acl_config['users'][username])
    acl_config = this._acl_config['users'][username];

  if (!acl_config) {
    if (options.require_always || this._acl_config && this._acl_config['default'] === 'deny')
      return [function(t,d){return false;}, function(d){return false;}];

    return [function(t,d){return true;}, function(d){return true;}];
  }
  var acl = new AccessControl(acl_config);
  // allowed(tablename, dbname), visible(dbname)
  return [function(t,d){return acl.allowed(t,d);}, function(d){return acl.visible(d);}];
};
