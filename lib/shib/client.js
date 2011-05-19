var Query = require('./query').Query,
    Result = require('./result').Result;

var thrift = require('thrift'),
    ttransport = require('thrift/transport'),
    ThriftHive = require('gen-nodejs/ThriftHive');

var kyotoclient = require('kyoto-client');

var HISTORY_KEY_PREFIX = "history:",
    KEYWORD_KEY_PREFIX = "keyword:",
    QUERY_KEY_PREFIX = "query:",
    RESULT_KEY_PREFIX = "result:";

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

var encodeIdList = function(str){return new Buffer(str, 'ascii');};
var decodeIdList = function(buf){return buf.toString('ascii');};
var encodeObject = function(str){return new Buffer(str, 'utf8');};
var decodeObject = function(buf){return buf.toString('utf8');};

var asciibuf = function(str){return new Buffer(str, 'ascii');};
var utf8buf = function(str){return new Buffer(str, 'utf8');};

var pad = function(n){return n < 10 ? '0'+n : n;};
var historyKey = function(){
  var d = new Date();
  return '' + d.getFullYear() + pad(d.getMonth());
};

Client.prototype.getKeys = function(type, callback){
  this.kyotoClient().matchPrefix(type, function(err, data){
    callback.apply(this, [err, data.map(function(v){return v.substr(type.length);})]);
  });
};

Client.prototype.getIds = function(type, key, callback){
  this.kyotoClient().get(type + key, function(err, data){
    if (data == null)
      callback.apply(this, [err, []]);
    else
      callback.apply(this, [err, decodeIdList(data).substr(1).split(',')]);
  });
};

Client.prototype.addId = function(type, key, id, callback){
  this.kyotoClient().append(type + key, encodeIdList(',' + id), function(err){
    if (callback)
      callback.apply(this, [err]);
  });
};

Client.prototype.getObject = function(type, key, callback){
  this.kyotoClient().get(type + key, function(err, data){
    if (data == null)
      callback.apply(this, [err, null]);
    else
      callback.apply(this, [err, decodeObject(data)]);
  });
};

Client.prototype.setObject = function(type, key, obj, callback){
  this.kyotoClient().set(type + key, encodeObject(obj), function(err){
    if (callback)
      callback.apply(this, [err]);
  });
};

Client.prototype.getHistories = function(callback){
  this.getKeys(HISTORY_KEY_PREFIX, callback);
};

Client.prototype.getHistory = function(yyyymm, callback){
  this.getIds(HISTORY_KEY_PREFIX, yyyymm, callback);
};

Client.prototype.addHistory = function(query){
  this.addId(HISTORY_KEY_PREFIX, historyKey(), query.queryid);
};

Client.prototype.getKeywords = function(callback){
  this.getKeys(KEYWORD_KEY_PREFIX, callback);
};

Client.prototype.getKeyword = function(keyword, callback){
  this.getIds(KEYWORD_KEY_PREFIX, keyword, callback);
};

Client.prototype.addKeyword = function(query){
  query.keywords.forEach(function(k){
    this.addId(KEYWORD_KEY_PREFIX, k, query.queryid);
  });
};

Client.prototype.getQuery = function(queryid, callback){
  this.getObject(QUERY_KEY_PREFIX, queryid, function(err, data){
    if (err)
      callback.apply(this, [err]);
    else
      callback.apply(this, [err, new Query({json:data})]);
  });
};

Client.prototype.createQuery = function(querystring, keywordlist, callback){
  try {
    var query = new Query({querystring:querystring, keywords:keywordlist});
    this.setObject(QUERY_KEY_PREFIX, query.queryid, query.serialized(), function(err){
      if (callback)
        callback.apply(this, [err, query]);
    });
  }
  catch (e) {
    if (callback)
      callback.apply(this, [e]);
  }
};

Client.prototype.execute = function(query){
  this.addHistory(query);
  this.addKeyword(query);

  this.hiveClient().execute(query.composed(), function(err, data){
    /* fetch and save results */
  });
};

Client.prototype.refresh = function(){
};

Client.prototype.status = function(){
};

Client.prototype.result = function(){
};
