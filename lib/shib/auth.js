var crypto = require('crypto');

var dumb = require('./auth/dumb')
  , http_basic_auth = require('./auth/http_basic_auth');

var CRYPTO_DEFAULT_CIPHER = 'aes192';

var passphrase = null;
var secret = null;

exports.provider = function(config){
  passphrase = crypto.pseudoRandomBytes(32).toString('binary');
  secret = crypto.pseudoRandomBytes(4).toString('hex');

  if (config === null)
    return new dumb.Auth();

  if (config.type === 'http_basic_auth')
    return new http_basic_auth.Auth(config);

  throw "unknown auth type name:" + config.type;
};

exports.crypto = function(username, password){
  var cipher = crypto.createCipher(CRYPTO_DEFAULT_CIPHER, passphrase);
  var data = secret + ':' + String(username) + ':' + String(password);
  cipher.update(data, 'utf8');
  var buf = cipher.final();
  return buf.toString('hex');
};

exports.decrypto = function(data){
  var user,pass;
  var buf = new Buffer(data, 'hex');
  var decipher = crypto.createDecipher(CRYPTO_DEFAULT_CIPHER, passphrase);
  decipher.update(buf);
  try {
    var source = decipher.final('utf8');
    var ary = source.split(':');
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
