var client = require('./client');

var server_confdata = {
  hiveserver: {
    host: 'localhost',
    port: 10000,
    setup_queries: []
  },
  kyototycoon: {
    host: 'localhost',
    port: 1978
  }
};
exports.init = function(arg){
  server_confdata = arg;
};

exports.client = function(arg){
  var conf = arg || server_confdata;
  return new client.Client(conf);
};
