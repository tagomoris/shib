var thrift = require('thrift'),
    ttransport = require('thrift/transport');
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
  waited_queries.push(queued_query);

  var timer = setInterval(function(){
    if (query != queued_query) {
      return;
    };
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
    val = query_result.length.shift();
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
