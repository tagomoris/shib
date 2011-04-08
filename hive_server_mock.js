var thrift = require('thrift'),
    ttransport = require('thrift/transport');
var ThriftHive = require('gen-nodejs/ThriftHive');

var mocks = 
};

var query = undefined;
var query_info = {};
var query_result = [];

var execute = function(query, success){
  /* hogehoge */
  success();
};
var getClusterStatus = function(success){};
var fetchOne = function(success){ success(returned_result); };
var fetchN = function(rows, success){ success(returned_results_rows); };
var fetchAll = function(success){ success(returned_results_all); };
var getSchema = function(success){};
var getThriftSchema = function(success){};
var getQueryPlan = function(success){};

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
