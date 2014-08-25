var crypto = require('crypto');

var dumb = require('./auth/dumb')
  , http_basic_auth = require('./auth/http_basic_auth');

var CRYPTO_DEFAULT_CIPHER = 'aes192';

var passphrase = null;
var secret = null;

var Auth = exports.Auth = function(config, logger){
  this.logger = logger;

  if (!passphrase)
    passphrase = crypto.pseudoRandomBytes(32).toString('binary');
  if (!secret)
    secret = crypto.pseudoRandomBytes(4).toString('hex');

  this._provider = null;

  this.enabled = false;
  this.require_always = config.require_always || false;
  this.realm = config.realm || '';

  if (config.type === null || config.type === undefined){
    this._provider = new dumb.Auth();
  } else if (config.type === 'http_basic_auth'){
    this._provider = new http_basic_auth.Auth(config, logger);
    this.enabled = true;
  } else {
    throw "unknown auth type name:" + config.type;
  }
};

Auth.prototype.check = function(req, callback){
  this._provider.check(req, callback);
};

Auth.prototype.credential = function(req){
  var user = this.userdata(req);
  var username = null;
  if (user)
    username = user.username;
  return new Credential(this._provider.acl_delegators(req, username, {require_always: this.require_always}));
};

Auth.prototype.userdata = function(req){
  var data = req.body.authInfo || req.get('X-Shib-AuthInfo');
  if (!data)
    return null;
  return this.decrypt(data);
};

Auth.prototype.crypto = function(username){
  var cipher = crypto.createCipher(CRYPTO_DEFAULT_CIPHER, passphrase);
  var data = secret + ':' + String(username);
  var buf0 = cipher.update(data, 'utf8', 'hex');
  var buf1 = cipher.final('hex');
  return buf0 + buf1;
};

Auth.prototype.decrypto = function(data){
  var user;
  var decipher = crypto.createDecipher(CRYPTO_DEFAULT_CIPHER, passphrase);
  var r0 = decipher.update(data, 'hex', 'utf8');
  try {
    var r1 = decipher.final('utf8');
    var ary = (r0 + r1).split(':');
    if (ary[0] === secret) {
      user = ary[1];
    }
  } catch (x) {
    // TypeError: error:06065064:digital envelope routines:EVP_DecryptFinal_ex:bad decrypt
    // decrypt error
  }
  if (!user)
    return null;
  return {username: user};
};

var Credential = function(acl_delegators){
  this.allowed_delegator = acl_delegators[0];
  this.visible_delegator = acl_delegators[1];
};

Credential.prototype.allowed = function(tablename, dbname){
  return this.allowed_delegator(tablename, dbname);
};

Credential.prototype.visible = function(dbname){
  return this.visible_delegator(dbname);
};
