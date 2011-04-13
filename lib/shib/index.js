var query = require('./query'),
    result = require('./result');

var thrift = require('thrift'),
    ttransport = require('thrift/transport'),
    ThriftHive = require('gen-nodejs/ThriftHive');

var kyotoclient = require('kyoto-client');

exports.createQuery = query.createQuery;

exports.getQuery = query.getQuery;
exports.getResult = result.getResult;

var confdata = {
  hiveserver: {
    host: 'localhost',
    port: 10000
  },
  kyototycoon: {
    host: 'localhost',
    port: 3000
  }
};
exports.init = function(arg){
  confdata = arg;
};

// querystring: string of query ('select f1,f1 from table where ...')
// resultdata: object from HiveServer as result of querystring

// queryid: md5 of query
// query: Query object from (queryid, querystring, [resultid1, resultid2, ...], last_executed_at, ...), stored in KT (shib.kch)
// resultid: md5 of (querystring + YYYYmmdd of executed date)
// result: Result object from (resultid, resultdata, queryid


var hiveClient = function(conf){
  var conn = thrift.createConnection(conf.host, conf.port, {transport: ttransport.TBufferedTransport}),
      client = thrift.createClient(ThriftHive, conn);
  return client;
};

var Client = exports.Client = function(args){
  this.conf = args || confdata;
};

var kyotoClient = function(conf){
  // conf.host, conf.port
};
