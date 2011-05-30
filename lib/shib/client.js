var Query = require('./query').Query,
    Result = require('./result').Result;

var thrift = require('thrift'),
    ttransport = require('thrift/transport'),
    ThriftHive = require('gen-nodejs/ThriftHive');

var kyotoclient = require('kyoto-client');

var KT_SHIB_DEFAULT = "shib.kch",
    KT_SHIB_RESULT = "result.kcd";

var HISTORY_KEY_PREFIX = "history:",
    KEYWORD_KEY_PREFIX = "keyword:",
    QUERY_KEY_PREFIX = "query:",
    RESULT_KEY_PREFIX = "result:";

var STATUS_LABEL_WAITING = "waiting",
    STATUS_LABEL_RUNNING = "running",
    STATUS_LABEL_DONE = "done",
    STATUS_LABEL_RERUNNING = "rerunning";

var HIVESERVER_READ_LINES = 100;

var LocalStoreError = exports.LocalStoreError = function(msg){
  this.name = 'LocalStoreError';
  Error.call(this, msg);
  Error.captureStackTrace(this, arguments.callee);
};
LocalStoreError.prototype.__proto__ = Error.prototype;

//TODO: write to throw LocalStoreError on err with KT operations

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
  this.hiveconnection = thrift.createConnection(
    this.conf.hiveserver.host,
    this.conf.hiveserver.port,
    {transport: ttransport.TBufferedTransport}
  );
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
var decodeIdList = function(buf){return buf && buf.toString('ascii');};
var encodeObject = function(str){return new Buffer(str, 'utf8');};
var decodeObject = function(buf){return buf && buf.toString('utf8');};

var pad = function(n){return n < 10 ? '0'+n : n;};
var historyKey = function(){
  var d = new Date();
  return '' + d.getFullYear() + pad(d.getMonth());
};

Client.prototype.getKeys = function(type, callback){
  this.kyotoClient().matchPrefix(type, KT_SHIB_DEFAULT, function(err, data){
    callback.apply(this, [err, data.map(function(v){return v.substr(type.length);})]);
  });
};

Client.prototype.getIds = function(type, key, callback){
  this.kyotoClient().get(type + key, KT_SHIB_DEFAULT, function(err, data){
    if (err || data == null)
      callback.apply(this, [err, []]);
    else
      callback.apply(this, [err, decodeIdList(data).substr(1).split(',')]);
  });
};

Client.prototype.addId = function(type, key, id, callback){
  this.kyotoClient().append(type + key, encodeIdList(',' + id), KT_SHIB_DEFAULT, function(err){
    if (callback)
      callback.apply(this, [err]);
  });
};

Client.prototype.getObject = function(type, key, callback){
  this.kyotoClient().get(type + key, KT_SHIB_DEFAULT, function(err, data){
    if (err || data == null)
      callback.apply(this, [err, null]);
    else
      callback.apply(this, [err, decodeObject(data)]);
  });
};

Client.prototype.setObject = function(type, key, obj, callback){
  this.kyotoClient().set(type + key, encodeObject(obj), KT_SHIB_DEFAULT, function(err){
    if (callback)
      callback.apply(this, [err]);
  });
};

Client.prototype.getHistory = function(yyyymm, callback){
  this.getIds(HISTORY_KEY_PREFIX, yyyymm, callback);
};
Client.prototype.history = Client.prototype.getHistory;
  
Client.prototype.getHistories = function(callback){
  this.getKeys(HISTORY_KEY_PREFIX, callback);
};
Client.prototype.histories = Client.prototype.getHistories;

Client.prototype.addHistory = function(query){
  this.addId(HISTORY_KEY_PREFIX, historyKey(), query.queryid);
};

Client.prototype.getKeyword = function(keyword, callback){
  this.getIds(KEYWORD_KEY_PREFIX, keyword, callback);
};
Client.prototype.keyword = Client.prototype.getKeyword;

Client.prototype.getKeywords = function(callback){
  this.getKeys(KEYWORD_KEY_PREFIX, callback);
};
Client.prototype.keywords = Client.prototype.getKeywords;

Client.prototype.addKeyword = function(query){
  if (query.keywords.length < 1)
    return;
  this.addId(KEYWORD_KEY_PREFIX, query.keywords[0], query.queryid);
};

Client.prototype.getQuery = function(queryid, callback){
  this.getObject(QUERY_KEY_PREFIX, queryid, function(err, data){
    if (err || data == null)
      callback.apply(this, [err, null]);
    else
      callback.apply(this, [err, new Query({json:data})]);
  });
};
Client.prototype.query = Client.prototype.getQuery;

Client.prototype.updateQuery = function(query, callback) {
  this.setObject(QUERY_KEY_PREFIX, query.queryid, query.serialized(), function(err){
    if (callback)
      callback.apply(this, [err]);
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

Client.prototype.getResult = function(resultid, callback){
  this.getObject(RESULT_KEY_PREFIX, resultid, function(err, data){
    if (err || data == null)
      callback.apply(this, [err, null]);
    else
      callback.apply(this, [err, new Result({json:data})]);
  });
};
Client.prototype.result = Client.prototype.getResult;

Client.prototype.setResult = function(result, callback){
  this.setObject(RESULT_KEY_PREFIX, result.resultid, result.serialized(), function(err){
    callback.apply(this, [err]);
  });
};

Client.prototype.getResultData = function(resultid, callback){
  this.getRawResultData(resultid, function(err, data){
    if (err || data == null)
      callback.apply(this, [err, null]);
    else
      callback.apply(this, [err, data]);
  });
};
Client.prototype.resultData = Client.prototype.getResultData;

Client.prototype.getRawResultData = function(resultid, callback){
  this.kyotoClient().get(resultid, KT_SHIB_RESULT, function(err, data){
    callback.apply(this, [err, decodeObject(data)]);
  });
};
Client.prototype.rawResultData = Client.prototype.getRawResultData;

Client.prototype.appendResultData = function(resultid, data, callback){
  this.kyotoClient().append(resultid, encodeObject(data.join("\n") + "\n"), KT_SHIB_RESULT, function(err){
    callback.apply(this, [err]);
  });
};

Client.prototype.refresh = function(query){
  this.execute(query, true);
};

Client.prototype.status = function(query){
  //TODO write!
};

Client.prototype.execute = function(query, refreshed){
  if (! refreshed) {
    this.addHistory(query);
    this.addKeyword(query);
  }

  var executed_at = (new Date()).toLocaleString();
  var result = new Result({queryid:query.queryid, executed_at:executed_at});
  this.setResult(result);

  query.results.push({executed_at:executed_at, resultid:result.resultid});
  this.updateQuery(query);
  
  this.hiveClient().execute(query.composed(), function(err, data){
    var client = this;
    var resultkey = result.resultid;
    var onerror = null;

    var resultfetch = function(callback) {
      client.hiveClient().fetchN(HIVESERVER_READ_LINES, function(err, data){
        if (err){
          onerror = err;
          return;
        }
        if (data.length < 1) {
          callback();
          return;
        }
        client.appendResultData(resultkey, data, function(err){
          if (err)
            throw new LocalStoreError("failed to append result data to KT");
          resultfetch(callback);
        });
      });
    };
    this.hiveClient().getSchema(function(err, data){
      if (err){
        onerror = err;
        return;
      }
      result.schema = data.fieldSchemas;
      resultfetch(function(){
        result.markAsExecuted(onerror);
        client.setResult(result);
      });
    });
  });
};
