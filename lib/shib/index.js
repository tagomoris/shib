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
  console.log("setup_queries");
  if (arg.hiveserver.setup_queries.length > 0) {
    var setups = arg.hiveserver.setup_queries.map(function(query){
      var q = query.trim();
      return q.trim() + (q.lastIndexOf(";") === q.length - 1 ? q : q + ";");
    });
    arg.setup_queries = setups.join('');
  }
  console.log(arg.setup_queries);
  server_confdata = arg;
};

exports.client = function(arg){
  var conf = arg || server_confdata;
  return new client.Client(conf);
};
