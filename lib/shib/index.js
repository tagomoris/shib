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
  if (arg.hiveserver.setup_queries.length > 0) {
    var setups = [];
    arg.hiveserver.setup_queries.forEach(function(query){
      var qlist = query.trim().split(";");
      qlist.forEach(function(q){
        if (q.length > 0)
          setups.push(q);
      });
    });
    arg.setup_queries = setups;
  }
  server_confdata = arg;
};

exports.client = function(arg){
  var conf = arg || server_confdata;
  return new client.Client(conf);
};
