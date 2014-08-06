var http_basic_auth = require('./auth/http_basic_auth');

exports.provider = function(config){
  if (config.type === 'http_basic_auth')
    return new http_basic_auth.Auth(config);
  return null;
};
