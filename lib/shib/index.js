var client = require('./client');

var confdata = {
  hiveserver: {
    host: 'localhost',
    port: 10000
  },
  kyototycoon: {
    host: 'localhost',
    port: 1978
  }
};
exports.init = function(arg){
  confdata = arg;
};

exports.client = function(arg){
  var conf = arg || confdata;
  return new client.Client(conf);
};
