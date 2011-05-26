var Query = require('./query').Query,
    ResultMeta = require('./result').ResultMeta;

var thrift = require('thrift'),
    ttransport = require('thrift/transport'),
    ThriftHive = require('gen-nodejs/ThriftHive');

var kyotoclient = require('kyoto-client');

var HISTORY_KEY_PREFIX = "history:",
    KEYWORD_KEY_PREFIX = "keyword:",
    QUERY_KEY_PREFIX = "query:",
    RESULTMETA_KEY_PREFIX = "result:";

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

Client.prototype.getHistory = function(yyyymm, callback){
  this.getIds(HISTORY_KEY_PREFIX, yyyymm, callback);
};

Client.prototype.getHistories = function(callback){
  this.getKeys(HISTORY_KEY_PREFIX, callback);
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
  if (query.keywords.length < 1)
    return;
  this.addId(KEYWORD_KEY_PREFIX, query.keywords[0], query.queryid);
};

Client.prototype.getQuery = function(queryid, callback){
  this.getObject(QUERY_KEY_PREFIX, queryid, function(err, data){
    if (err)
      callback.apply(this, [err]);
    else
      callback.apply(this, [err, new Query({json:data})]);
  });
};

Client.prototype.updateQuery = function(query, callback) {
  this.setObject(QUERY_KEY_PREFIX, query.queryid, query.serialized(), function(err){
    if (callback)
      callback.apply(this, [err, query]);
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

Client.prototype.getQuery = function(queryid, callback){
  if (queryid instanceof Array)
    this.kyotoClient().getBulk(queryid.map(function(k){return "queries:"+k;}), function(err, results){
      callback.apply(this, [err, queryid.map(function(id){return new Query({json:decodeObject(results["queries:"+id])});})]);
    });
  else
    this.kyotoClient().get("queries:" + queryid, function(err, value){
      callback.apply(this, [err, new Query({json:decodeObject(value)})]);
    });
};

Client.prototype.execute = function(query){
  this.addHistory(query);
  this.addKeyword(query);

  var executed_at = (new Date()).toLocaleString();
  var resultmeta = new ResultMeta({queryid:query.queryid, executed_at:executed_at});
  this.setObject(RESULTMETA_KEY_PREFIX, resultmeta.resultid, resultmeta.serialized());

  query.results.push({executed_at:executed_at, resultid:resultmeta.resultid});
  this.updateQuery(query);
  
  this.hiveClient().execute(query.composed(), function(err, data){
    /* fetch and save results */
    // use fetchN ?
    this.hiveClient().fetchAll(function(err, data){
      if (err){
        resultmeta.markAsExecuted(err);
        this.setObject(RESULTMETA_KEY_PREFIX, resultmeta.resultid, resultmeta.serialized());
        return;
      }

      // save results
    });
  });
};

Client.prototype.refresh = function(){
};

Client.prototype.status = function(){
};

Client.prototype.result = function(){
};
