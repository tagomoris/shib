var thrift = require('thrift'),
    ttransport = require('thrift/lib/thrift/transport');
var ThriftHive = require('gen-nodejs/ThriftHive');

var mock = require('ThriftHiveMock');
/*
 * ThriftHive server mock with dummy response,
 *  cannot handle concurrent connections...
 *  cannot send response with exceptions...
 */

var query = undefined;
var query_plan = undefined;
var query_schema = undefined;
var query_result = [];

var waited_queries = [];

var init_status = function(){
  query_plan = query_schema = query = undefined;
  query_result = [];
};

setInterval(function(){
  if (query != undefined || waited_queries.length < 1) {
    return;
  }
  query_plan = mock.query_plan(waited_queries[0]);
  query_schema = mock.schema(waited_queries[0]);
  query_result = mock.result(waited_queries[0]);
  query = waited_queries.shift();
}, 250);

var execute = function(queued_query, success){
  var q = queued_query.split('\n').join(' ');
  var executeDelay = 0;
  var matched = null;
  console.log('=================================================');
  console.log(q);
  if ((matched = /-- sleep ([0-9]+)$/im.exec(queued_query)) !== null) {
    executeDelay = Number(matched[1]) * 1000;
    console.log('sleep detected:' + executeDelay);
  }
  console.log('=================================================');
  setTimeout(function(){
    waited_queries.push(q);
    console.log("query pushed:" + q);
  }, executeDelay);

  var timer = setInterval(function(){
    if (query != q) {
      return;
    };
    console.log('target query executed');
    clearInterval(timer);
    success();
  }, 500);
};

var getClusterStatus = function(success){
  success(mock.cluster_status());
};

var fetchOne = function(success){
  if (query == undefined) {
    success('');
    return;
  }
  var val = '';
  if (query_result.length > 0) {
    val = query_result.shift();
  }
  if (query_result.length == 0) {
    init_status();
  }
  success(val);
};

var fetchN = function(rows, success){
  if (query == undefined) {
    success(['']);
    return;
  }
  var val = [];
  for (var i = 0; i < rows; i++) {
    val.push(query_result.shift());
    if (query_result.length < 1) {
      init_status();
      break;
    }
  }
  success(val);
};

var fetchAll = function(success){
  if (query == undefined) {
    success(['']);
    return;
  }
  var val = query_result;
  init_status();
  success(val);
};

var getSchema = function(success){
  if (query_schema == undefined) {
    success(mock.schema());
    return;
  }
  success(query_schema);
};

var getThriftSchema = function(success){
  getSchema(success);
};

var getQueryPlan = function(success){
  if (query_plan == undefined) {
    success(mock.query_plan());
  }
  success(query_plan);
};

var server_mock = thrift.createServer(ThriftHive, {
  getClusterStatus: getClusterStatus,
  execute: execute,
  fetchOne: fetchOne,
  fetchN: fetchN,
  fetchAll: fetchAll,
  getSchema: getSchema,
  getThriftSchema: getThriftSchema,
  getQueryPlan: getQueryPlan
}, {transport: ttransport.TBufferedTransport});
server_mock.listen(10000);
