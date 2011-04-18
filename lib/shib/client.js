var Query = require('./query').Query,
    Result = require('./result').Result;

var thrift = require('thrift'),
    ttransport = require('thrift/transport'),
    ThriftHive = require('gen-nodejs/ThriftHive');

var kyotoclient = require('kyoto-client');

var Client = exports.Client = function(args){
  this.conf = args;

  this.hiveconnection = undefined;
  this.hiveclient = undefined;
  this.kyotoclient = undefined;
};

Client.prototype.hiveClient = function(){
  if (this.hiveconnection && this.hiveclient) {
    return this.hiveclient;
  }
  this.hiveconnection = thrift.createConnection(this.conf.hiveserver.host, this.conf.hiveserver.port, {transport: ttransport.TBufferedTransport});
  this.hiveclient = thrift.createClient(ThriftHive, this.hiveconnection);
  return this.hiveclient;
};

Client.prototype.kyotoClient = function(){
  if (this.kyotoclient) {
    return this.kyotoclient;
  }
  this.kyotoclient = new kyotoclient.Db(this.conf.kyototycoon.host, this.conf.kyototycoon.port);
  this.kyotoclient.open();
  return this.kyotoclient;
};

Client.prototype.end = function(){
  if (this.hiveconnection) {
    this.hiveconnection.end();
    this.hiveconnection = this.hiveclient = undefined;
  }
  if (this.kyotoclient) {
    this.kyotoclient.close();
    this.kyotoclient = undefined;
  }
};

// querystring: string of query ('select f1,f1 from table where ...')
// resultdata: object from HiveServer as result of querystring

// queryid: md5 of query
// query: Query object, stored in KT (shib.kch)
//   (queryid, querystring, servicename, {executed_date1:resultid1, executed_date2:resultid2, ...}, last_executed_at, ...)
// resultid: md5 of (querystring + YYYYmmdd of executed date)
// result: Result object from (resultid, resultdata, queryid

var pad = function(n){return n < 10 ? '0'+n : n;};
var historyKey = function(){
  var d = new Date();
  return '' + d.getFullYear() + pad(d.getMonth());
};

Client.prototype.addHistory = function(query){
  this.kyotoClient().append("history:" + historyKey(), ',' + query.queryid);
};

Client.prototype.addService = function(query){
  this.kyotoClient().append("service:" + query.service, ',' + query.queryid);
};

Client.prototype.createQuery = function(service, querystring, callback){
  this.callback = callback;
  try {
    var query = Query(querystring, service);
    this.kyotoClient().set("queries:" + query.queryid, query.serialized(), function(err, data){
      if (this.callback)
        this.callback(undefined, query);
    });
  }
  catch (e) {
    if (this.callback)
      this.callback(e, undefined);
  }
};

Client.prototype.getQuery = function(){
};

/*
 History
 Service
 List of History (201012, 201101, 201102, ...)
 List of Service
 */

Client.prototype.execute = function(){
      this.addService(service, query);
      this.addHistory(query);
};

Client.prototype.refresh = function(){
};

Client.prototype.status = function(){
};

Client.prototype.result = function(){
};
