var client = require('./client');

var default_configs = {
  localdisk: {
    datadir: './var'
  },
  hiveserver: {
    version: 1, // or 2
    host: 'localhost',
    port: 10000,
    support_database: true,
    default_database: 'default',
    setup_queries: []
  },
  huahinmanager: {
    enable: true,
    host: 'localhost',
    port: 9010,
    mapreduce: 'MRv1' // or 'YARN'
  }
};

var config = null;

exports.init = function(arg){
  var merge = function(destination, source) {
    for (var property in source) {
      if (source.hasOwnProperty(property)) {
        destination[property] = source[property];
      }
    }
    return destination;
  };
  config = merge(merge({}, default_configs), arg);
};

exports.client = function(arg){
  return new client.Client(config);
};
