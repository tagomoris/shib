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

// resultid: md5 of (queryid + YYYYmmddHHMMSS of executed date)
// result: Result object from (resultid, queryid, executed_at, resultdata)

var encodeKeyList = function(str){return new Buffer(str, 'ascii');};
var decodeKeyList = function(buf){return buf.toString('ascii');};
var encodeObject = function(str){return new Buffer(str, 'utf8');};
var decodeObject = function(buf){return buf.toString('utf8');};

var asciibuf = function(str){return new Buffer(str, 'ascii');};
var utf8buf = function(str){return new Buffer(str, 'utf8');};

var pad = function(n){return n < 10 ? '0'+n : n;};
var historyKey = function(){
  var d = new Date();
  return '' + d.getFullYear() + pad(d.getMonth());
};

Client.prototype.getHistories = function(callback){
  this.callback = callback;
  this.kyotoClient().matchPrefix("history:", function(err, data){
    this.callback(err, data.map(function(v){return v.substr(8);}));
  });
};

Client.prototype.getHistory = function(yyyymm, callback){}; /* ** */

Client.prototype.addHistory = function(query){
  this.kyotoClient().append("history:" + historyKey(), encodeKeyList(',' + query.queryid), function(){});
};

Client.prototype.addKeyword = function(query){
  this.kyotoClient().append("keyword:" + query.keyword, encodeKeyList(',' + query.queryid), function(){});
};

Client.prototype.createQuery = function(querystring, keywordlist, callback){
  this.callback = callback;
  try {
    var query = Query({querystring:querystring, keywords:keywordlist});
    this.kyotoClient().set("queries:" + query.queryid, encodeObject(query.serialized()), function(err, data){
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
 Keyword
 List of History (201012, 201101, 201102, ...)
 List of Service
 */

Client.prototype.execute = function(){
      this.addKeyword(keyword, query);
      this.addHistory(query);
};

Client.prototype.refresh = function(){
};

Client.prototype.status = function(){
};

Client.prototype.result = function(){
};
