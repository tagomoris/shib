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

var STATUS_LABEL_RUNNING = "running",
    STATUS_LABEL_DONE = "done",
    STATUS_LABEL_RERUNNING = "re-running";

var HIVESERVER_READ_LINES = 100;

var LocalStoreError = exports.LocalStoreError = function(msg){
  this.name = 'LocalStoreError';
  Error.call(this, msg);
  Error.captureStackTrace(this, arguments.callee);
};
LocalStoreError.prototype.__proto__ = Error.prototype;

var Client = exports.Client = function(args){
  this.conf = args;

  this.hiveconnection = undefined;
  this.hiveclient = undefined;
  this.kyotoclient = undefined;
  this.setup_queries = this.conf.setup_queries || [];
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
  return '' + d.getFullYear() + pad(d.getMonth() + 1);
};

function error_callback(t, callback, err, data){
  if (! callback) return;
  if (data && data['ERROR'])
    err.message += ' ERROR:' + data['ERROR'];
  callback.apply(t, [err]);
};

Client.prototype.getKeys = function(type, callback){
  var client = this;
  this.kyotoClient().matchPrefix(type, null, {database: KT_SHIB_DEFAULT}, function(err, data){
    if (err)
      error_callback(client, callback, err, data);
    else
      callback.apply(client, [err, (data && data.map(function(v){return v.substr(type.length);}))]);
  });
};

Client.prototype.getIds = function(type, key, callback){
  var client = this;
  this.kyotoClient().get(type + key, {database: KT_SHIB_DEFAULT}, function(err, data){
    if (err)
      error_callback(client, callback, err, data);
    else {
      var idlistStr = decodeIdList(data);
      var idlist = [];
      if (idlistStr !== null ) {
        idlist = idlistStr.substr(1).split(',');
      }
      callback.apply(client, [err, idlist]);
    }
  });
};

Client.prototype.setIds = function(type, key, ids, callback){
  var client = this;
  this.kyotoClient().set(type + key, ',' + ids.join(','), {database: KT_SHIB_DEFAULT}, function(err){
    if (err)
      error_callback(client, callback, err);
    else if (callback)
      callback.apply(client, [err]);
  });
};

Client.prototype.getIdsBulk = function(type, keys, callback){
  var client = this;
  var bulkkeys = keys.map(function(v){return type + v;});
  this.kyotoClient().getBulk(bulkkeys, {database: KT_SHIB_DEFAULT}, function(err, data){
    if (err)
      error_callback(client, callback, err, data);
    else
      callback.apply(client, [err, bulkkeys.map(function(k){return decodeIdList(data[k]).substr(1).split(',');})]);
  });
};

Client.prototype.addId = function(type, key, id, callback){
  var client = this;
  this.kyotoClient().append(type + key, encodeIdList(',' + id), {database: KT_SHIB_DEFAULT}, function(err){
    if(err)
      error_callback(client, callback, err);
    else if (callback)
      callback.apply(client, [err]);
  });
};

Client.prototype.removeId = function(type, key, id, callback){
  var client = this;
  this.getIds(type, key, function(err, idList) {
    if (err)
      error_callback(client, callback, err);
    else {
      var removedList = idList.filter(function(v){return v !== id;});
      if (removedList.length === idList.length) {
        if (callback)
          callback.apply(client, [null]);
        return;
      }
      client.setIds(type, key, removedList, function(err){
        if (err)
          error_callback(client, callback, err);
        else if (callback)
          callback.apply(client, [err]);
      });
    }
  });
};

Client.prototype.getObject = function(type, key, callback){
  var client = this;
  this.kyotoClient().get(type + key, {database: KT_SHIB_DEFAULT}, function(err, data){
    if (err)
      error_callback(client, callback, err, data);
    else
      callback.apply(client, [err, decodeObject(data)]);
  });
};

Client.prototype.getObjects = function(type, keys, callback){
  var client = this;
  if (! keys) {
    callback.apply(client, [null, []]);
    return;
  }
  var objkeys = keys.map(function(v){return type + v;});
  this.kyotoClient().getBulk(objkeys, {database: KT_SHIB_DEFAULT}, function(err, data){
    if (err)
      error_callback(client, callback, err, data);
    else
      callback.apply(client, [err, objkeys.map(function(k){return decodeObject(data[k]);})]);
  });
};

Client.prototype.setObject = function(type, key, obj, callback){
  var client = this;
  this.kyotoClient().set(type + key, encodeObject(obj), {database: KT_SHIB_DEFAULT}, function(err){
    if (err)
      error_callback(client, callback, err);
    if (callback)
      callback.apply(client, [err]);
  });
};

Client.prototype.removeObject = function(type, key, callback){
  var client = this;
  this.kyotoClient().remove(type + key, {database: KT_SHIB_DEFAULT}, function(err){
    if (err)
      error_callback(client, callback, err);
    else if (callback)
      callback.apply(client, [err]);
  });
};

Client.prototype.getHistory = function(yyyymm, callback){
  this.getIds(HISTORY_KEY_PREFIX, yyyymm, callback);
};
Client.prototype.history = Client.prototype.getHistory;
  
Client.prototype.getHistoryBulk = function(yyyymmlist, callback){
  this.getIdsBulk(HISTORY_KEY_PREFIX, yyyymmlist, callback);
};

Client.prototype.getHistories = function(callback){
  this.getKeys(HISTORY_KEY_PREFIX, callback);
};
Client.prototype.histories = Client.prototype.getHistories;

Client.prototype.addHistory = function(query){
  this.addId(HISTORY_KEY_PREFIX, historyKey(), query.queryid);
};

Client.prototype.removeHistory = function(historyKey, queryid, callback){
  this.removeId(HISTORY_KEY_PREFIX, historyKey, queryid, callback);
};

Client.prototype.getKeyword = function(keyword, callback){
  this.getIds(KEYWORD_KEY_PREFIX, keyword, callback);
};
Client.prototype.keyword = Client.prototype.getKeyword;

Client.prototype.getKeywordBulk = function(keywordlist, callback){
  this.getIdsBulk(KEYWORD_KEY_PREFIX, keywordlist, callback);
};

Client.prototype.getKeywords = function(callback){
  this.getKeys(KEYWORD_KEY_PREFIX, callback);
};
Client.prototype.keywords = Client.prototype.getKeywords;

Client.prototype.addKeyword = function(query){
  if (query.keywords.length < 1)
    return;
  this.addId(KEYWORD_KEY_PREFIX, query.keywords[0], query.queryid);
};

Client.prototype.removeKeyword = function(keyword, queryid, callback){
  this.removeId(KEYWORD_KEY_PREFIX, keyword, queryid, callback);
};

Client.prototype.getQuery = function(queryid, callback){
  var client = this;
  this.getObject(QUERY_KEY_PREFIX, queryid, function(err, data){
    if (err)
      error_callback(client, callback, err, data);
    else
      callback.apply(client, [err, (data && new Query({json:data}))]);
  });
};
Client.prototype.query = Client.prototype.getQuery;

Client.prototype.getQueries = function(queryids, callback){
  var client = this;
  this.getObjects(QUERY_KEY_PREFIX, queryids, function(err, data){
    if (err)
      error_callback(client, callback, err, data);
    else
      callback.apply(client, [err, data.filter(function(v){return v && v.length > 1;}).map(function(v){return new Query({json:v});})]);
  });
};
Client.prototype.queries = Client.prototype.getQueries;

Client.prototype.updateQuery = function(query, callback) {
  var client = this;
  this.setObject(QUERY_KEY_PREFIX, query.queryid, query.serialized(), function(err){
    if (err)
      error_callback(client, callback, err);
    else if (callback)
      callback.apply(client, [err]);
  });
};

Client.prototype.createQuery = function(querystring, keywordlist, callback){
  var client = this;
  try {
    var today = new Date();
    var seedYYYYMMDD = '' + today.getFullYear() + today.getMonth() + today.getDay(); // seed is not needed strictlicity
    var query = new Query({querystring:querystring, keywords:keywordlist, seed: seedYYYYMMDD});
    client.query(query.queryid, function(err, savedquery){
      if (!err && savedquery) {
        callback.apply(client, [err, savedquery]);
        return;
      }
      this.setObject(QUERY_KEY_PREFIX, query.queryid, query.serialized(), function(err){
        if (err)
          error_callback(client, callback, err);
        else
          callback.apply(client, [err, query]);
      });
    });
  }
  catch (e) {
    error_callback(client, callback, e);
  }
};

Client.prototype.deleteQuery = function(queryid, callback){
  var client = this;
  this.removeObject(QUERY_KEY_PREFIX, queryid, function(err){
    if (err)
      error_callback(client, callback, err);
    else if (callback)
      callback.apply(client, [err]);
  });
};

Client.prototype.getResult = function(resultid, callback){
  var client = this;
  this.getObject(RESULT_KEY_PREFIX, resultid, function(err, data){
    if (err)
      error_callback(client, callback, err, data);
    else if (data)
      callback.apply(client, [err, new Result({json:data})]);
    else
      callback.apply(client, [err, null]);
  });
};
Client.prototype.result = Client.prototype.getResult;

Client.prototype.getResults = function(resultids, callback){
  var client = this;
  this.getObjects(RESULT_KEY_PREFIX, resultids, function(err, data){
    if (err)
      error_callback(client, callback, err, data);
    else
      callback.apply(client, [err, data.map(function(v){return v && new Result({json:v});})]);
  });
};
Client.prototype.results = Client.prototype.getResults;

Client.prototype.setResult = function(result, callback){
  var client = this;
  this.setObject(RESULT_KEY_PREFIX, result.resultid, result.serialized(), function(err){
    if (err)
      error_callback(client, callback, err);
    else if (callback)
      callback.apply(client, [err]);
  });
};

Client.prototype.getResultData = function(resultid, callback){
  var client = this;
  this.getRawResultData(resultid, function(err, data){
    if (err) {
      error_callback(client, callback, err, data);
      return;
    }

    var list = [];
    data.split("\n").forEach(function(line){
      if (line == "")
        return;
      list.push(line.split("\t"));
    });
    callback.apply(client, [err, list]);
  });
};
Client.prototype.resultData = Client.prototype.getResultData;

Client.prototype.getRawResultData = function(resultid, callback){
  var client = this;
  this.kyotoClient().get(resultid, {database: KT_SHIB_RESULT}, function(err, data){
    if (err)
      error_callback(client, callback, err, data);
    else
      callback.apply(client, [err, decodeObject(data)]);
  });
};
Client.prototype.rawResultData = Client.prototype.getRawResultData;

Client.prototype.appendResultData = function(resultid, data, callback){
  var client = this;
  this.kyotoClient().append(resultid, encodeObject(data.join("\n") + "\n"), {database: KT_SHIB_RESULT}, function(err){
    if (err)
      error_callback(client, callback, err);
    else if(callback)
      callback.apply(client, [err]);
  });
};

Client.prototype.getLastResult = function(query, callback){
  var client = this;
  if (query.results.length < 1){
    callback.apply(client, [undefined, null]);
    return;
  }
  this.getResults(query.results.reverse().map(function(v){return v.resultid;}), function(err, results){
    if (err){
      error_callback(client, callback, err);
      return;
    }
    var r;
    while((r = results.shift()) !== undefined){
      if (r.running())
        continue;
      callback.apply(client, [undefined, r]);
      return;
    }
    callback.apply(client, [undefined, null]);
  });
};

Client.prototype.status = function(query, callback){
  var client = this;
  /*
   callback argument
   running: newest-and-only query running, and result not stored yet.
   executed (done): newest query executed, and result stored.
   error: newest query executed, but done with error.
   re-running: newest query running, but older result exists.
   */
  if (query.results.length < 1) {
    callback.apply(client, ["running"]);
    return;
  }
  var resultid_revs = query.results.reverse().map(function(v){return v.resultid;});
  this.getResults(resultid_revs, function(err, results){
    if (! results.every(function(element, index, array){return element !== null && element !== undefined;}))
      throw new LocalStoreError("Result is null for one or more ids of: " + resultid_revs.join(","));

    var newest = results.shift();
    if (newest.running()){
      if (results.length < 1)
        callback.apply(client, ["running"]);
      else {
        var alter = results.shift();
        if (! alter.running() && ! alter.withError())
          callback.apply(client, ["re-running"]);
        else
          callback.apply(client, ["running"]);
      }
    }
    else if (newest.withError())
      callback.apply(client, ["error"]);
    else
      callback.apply(client, ["executed"]);
  });
};

/* execute query without all of query checks, keywords, history-keyword-saving and result-caching */
Client.prototype.executeSystemStatement = function(quoted_query, callback){
  var client = this;
  client.hiveClient().execute(quoted_query, function(err, data){
    client.hiveClient().fetchAll(function(err, result){
      if (err) {
        callback.apply(client, [err]);
        return;
      }
      callback.apply(client, [null, result]);
    });
  });
};

Client.prototype.giveup = function(query, callback){
  var client = this;
  var result = query.results[query.results.length - 1];
  if (result === undefined){
    var executed_at = (new Date()).toLocaleString();
    result = new Result({queryid:query.queryid, executed_at:executed_at});
    result.markAsExecuted({message: 'specified as "give up"'});
    this.setResult(result, function(){
      query.results.push({executed_at:executed_at, resultid:result.resultid});
      this.updateQuery(query);
      if (callback)
        callback.apply(client, [null, query]);
    });
    return;
  }
  var resultid = result.resultid;
  this.result(resultid, function(err, result){
    result.markAsExecuted({message: 'specified as "give up"'});
    client.setResult(result, function(err){
      if (callback)
        callback.apply(client, [err, query]);
    });
  });
};

Client.prototype.setupClient = function(setups, callback){
  var client = this;

  var setupQueue = setups.concat();
  var executeSetup = function(q, callback){
    client.hiveClient().execute(q, function(err) {
      if (err)
        callback(err, null);
      else
        client.hiveClient().fetchAll(function(err, data){
          if (setupQueue.length > 0)
            executeSetup(setupQueue.shift(), callback);
          else
            callback(null, client);
        });
    });
  };
  executeSetup(setupQueue.shift(), callback);
};

Client.prototype.execute = function(query, args){
  if (! args)
    args = {};
  this.addHistory(query);
  this.addKeyword(query);

  var client = this;

  var executed_at = (new Date()).toLocaleString();
  var result = new Result({queryid:query.queryid, executed_at:executed_at});
  this.setResult(result, function(){
    query.results.push({executed_at:executed_at, resultid:result.resultid});
    this.updateQuery(query);
  
    if (args.prepare) args.prepare(query);

    this.setupClient(this.conf.setup_queries, function(err, client) {
      if (err || !client) {
        result.markAsExecuted(err);
        client.setResult(result);
        if (args.error) args.error(query);
        return;
      }
      client.hiveClient().execute(query.composed(), function(err, data){
        if (args.broken && args.broken(query)) {
          return;
        }
        if (err) {
          result.markAsExecuted(err);
          client.setResult(result);
          if (args.error) args.error(query);
          return;
        }

        var resultkey = result.resultid;
        var resultLines = 0;
        var resultBytes = 0;
        var schemaRow = null;
        var onerror = null;

        var resultfetch = function(callback) {
          client.hiveClient().fetchN(HIVESERVER_READ_LINES, function(err, data){
            if (err){
              onerror = err;
              return;
            }
            if (data.length < 1 || data.length == 1 && data[0].length < 1){
              callback.apply(client, []);
              return;
            }
            if (schemaRow) {
              data.unshift(schemaRow);
              schemaRow = null;
            }
            client.appendResultData(resultkey, data, function(err){
              if (err)
                throw new LocalStoreError("failed to append result data to KT");
              resultLines += data.length;
              resultBytes += data.reduce(function(prev,v){return prev + v.length + 1;}, 0);
              resultfetch(callback);
            });
          });
        };
        client.hiveClient().getSchema(function(err, data){
          if (err){
            onerror = err;
            return;
          }
          result.schema = data.fieldSchemas;
          schemaRow = result.schema.map(function(f){return f.name.toUpperCase();}).join('\t');
          resultfetch(function(){
            result.markAsExecuted(onerror);
            result.lines = resultLines;
            result.bytes = resultBytes;
            client.setResult(result);

            if (onerror && args.error)
              args.error(query);
            else if (args.success)
              args.success(query);
          });
        });
      });
    });
  });
};
