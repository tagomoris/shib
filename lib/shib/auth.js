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

Auth.prototype.check = function(username, password, callback){
  this._provider.check(username, password, callback);
};

Auth.prototype.crypto = function(username, password){
  var cipher = crypto.createCipher(CRYPTO_DEFAULT_CIPHER, passphrase);
  var data = secret + ':' + String(username) + ':' + String(password);
  var buf0 = cipher.update(data, 'utf8', 'hex');
  var buf1 = cipher.final('hex');
  return buf0 + buf1;
};

Auth.prototype.decrypto = function(data){
  var user,pass;
  var decipher = crypto.createDecipher(CRYPTO_DEFAULT_CIPHER, passphrase);
  var r0 = decipher.update(data, 'hex', 'utf8');
  try {
    var r1 = decipher.final('utf8');
    var ary = (r0 + r1).split(':');
    if (ary[0] === secret) {
      user = ary[1];
      pass = ary[2];
    }
  } catch (x) {
    // TypeError: error:06065064:digital envelope routines:EVP_DecryptFinal_ex:bad decrypt
    // decrypt error
  }
  if (!user || !pass)
    return null;
  return {username: user, password: pass};
};
