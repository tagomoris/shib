var AccessControl = require('shib/access_control').AccessControl;

/*
var auth = {
  require_always: true,
  type: 'http_custom_header',
  realm: 'linecorp rev',
  username: 'X-Shib-Auth-User',
  groupname: 'X-Shib-Auth-Group',
  access_control: {
    users: {
      tagomoris: {
        default: "deny",
        databases: {
          default: { default: "allow" },
          legy: { default: "deny", allow: ["access_logs"] }
        }
      },
      hogepos: {
        default: "allow"
      }
    },
    groups: {
      supermember: {
        default: "allow"
      },
      limitedmember: {
        default: "deny",
        databases: {
          default: { default: "allow" }
        }
      }
    },
    order: ["group", "user"], //default
    default: "deny"  //default
  }
};
*/

var Auth = exports.Auth = function(args, logger){
  this.logger = logger;
  this.realm = args.realm;

  if (! args.require_always)
    throw "Auth 'http_custom_headers' does not permit 'require_always: false'";

  if (! args.username)
    throw "Auth 'http_custom_headers' requires username";

  this._username = args.username;
  this._groupname = args.groupname;
  this._acl_config = args.access_control;
};

Auth.prototype.check = function(req, callback) {
  var username = req.get(this._username);
  var groupname = null;
  if (this._groupname)
    groupname = req.get(this._groupname);

  if (!username && !groupname) {
    callback(null, false);
    return;
  }
  callback(null, {username: username});
};

// On this auth plugin, HTTP custom headers is the most important credential,
// rather than Shib auth headers.
Auth.prototype.acl_delegators = function(req, username, options) {
  // [ allowed(tablename, dbname), visible(dbname) ]

  if (!username)
    username = req.get(this._username);

  if (!username || username !== req.get(this._username))
    return [function(t,d){return false;}, function(d){return false;}];

  var groupname = null;
  if (this._groupname)
    groupname = req.get(this._groupname);

  var aclconfig = this._acl_config;

  var order = (aclconfig['order'] || ["group","user"]);
  var acl = null;
  order.forEach(function(order_item){
    if (acl)
      return; // continue

    if (order_item === "group") {
      if (! groupname)
        return; // continue
      var gdata = (aclconfig['groups'] || {})[groupname];
      if (gdata)
        acl = new AccessControl(gdata);

    } else { // user
      var udata = (aclconfig['users'] || {})[username];
      if (udata)
        acl = new AccessControl(udata);
    }
  });
  if (acl) 
    return [function(t,d){return acl.allowed(t,d);}, function(d){return acl.visible(d);}];

  if (aclconfig['default'] === "allow")
    return [function(t,d){return true;}, function(d){return true;}];

  // default deny
  return [function(t,d){return false;}, function(d){return false;}];
};
