var config = {
  hiveserver: {
    host: 'localhost',
    port: 10000
  }
};

var thrift = require('thrift'),
    ttransport = require('thrift/transport'),
    ThriftHive = require('gen-nodejs/ThriftHive');

var connection = thrift.createConnection(
  config.hiveserver.host,
  config.hiveserver.port,
  {transport: ttransport.TBufferedTransport}
);
var hiveclient = thrift.createClient(ThriftHive, connection);

var query = "SELECT hhmm, (split(fullpath,'\/'))[1] AS blogname, count(userlabel) AS cnt " +
  "FROM access_log WHERE service='blog' AND yyyymmdd='20110531' " +
  "GROUP BY hhmm, blogname SORT BY cnt DESC limit 100";

hiveclient.execute(query, function(err, data){
  hiveclient.getSchema(function(err, data){
    console.log("schema:");
    console.log(data);
    var i = 1;
    var fetchNext = function(callback){
      hiveclient.fetchN(30, function(err, data){
        console.log("fetch " + i + " err:" + err);
        console.log("fetch " + i + " data:" + data.length);
        console.log(data.map(function(v){return v.split("\t");}));
        i += 1;
        if (!err && data && data.length > 0 && ! (data.length == 1 && data[0].length < 1))
          fetchNext(callback);
        else
          callback();
      });
    };
    fetchNext(function(){console.log("fetched times:" + (i - 1));});
  });
});
