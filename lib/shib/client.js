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

Client.prototype.getHistory = function(yyyymm, callback){
  this.callback = callback;
  this.kyotoClient().get("history:" + yyyymm, function(err, data){
    this.callback(err, data.split(',').splice(1));
  });
};

Client.prototype.addHistory = function(query){
  this.kyotoClient().append("history:" + historyKey(), encodeKeyList(',' + query.queryid), function(){});
};

Client.prototype.getKeywords = function(callback){
  this.callback = callback;
  this.kyotoClient().matchPrefix("keyword:", function(err, data){
    this.callback(err, data.map(function(v){return v.substr(8);}));
  });
};

Client.prototype.getKeyword = function(keyword, callback){
  this.callback = callback;
  this.kyotoClient().get("keyword:" + keyword, function(err, data){
    this.callback(err, data.split(',').splice(1));
  });
};

Client.prototype.addKeyword = function(query){
  if (query.keywords.length < 1)
    return;
  this.kyotoClient().append("keyword:" + query.keywords[0], encodeKeyList(',' + query.queryid), function(){});
};

Client.prototype.createQuery = function(querystring, keywordlist, callback){
  this.callback = callback;
  try {
    var query = Query({querystring:querystring, keywords:keywordlist});
    this.kyotoClient().set("queries:" + query.queryid, encodeObject(query.serialized()), function(err){
      this.addKeyword(query);
      this.addHistory(query);
      if (this.callback)
        this.callback(err, query);
    });
  }
  catch (e) {
    if (this.callback)
      this.callback(e, undefined);
  }
};

Client.prototype.getQuery = function(queryid, callback){
  this.callback = callback;
  if (queryid instanceof Array)
    this.kyotoClient().getBulk(queryid.map(function(k){return "queries:"+k;}), function(err, results){
      this.callback(err, queryid.map(function(id){return new Query({json:decodeObject(results["queries:"+id])});}));
    });
  else
    this.kyotoClient().get("queries:" + queryid, function(err, value){
      this.callback(err, new Query({json:decodeObject(value)}));
    });
};

Client.prototype.execute = function(query){
  this.hiveClient().execute(query);
};

Client.prototype.refresh = function(){
};

Client.prototype.status = function(){
};

Client.prototype.result = function(){
};
