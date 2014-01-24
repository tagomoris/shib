var client = require('./client');

var default_configs = {
  listen: 3000,
  fetch_lines: 1000,
  query_timeout: null,
  setup_queries: [],
  storage: {
    datadir: './var'
  },
  engines: [
    {
      label: 'hive',
      executer: {
        name: 'hiveserver',
        host: 'localhost',
        port: 10000,
        support_database: true,
        default_database: 'default',
        query_timeout: null,
        setup_queries: []
      },
      monitor: null
    }
  ]
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
