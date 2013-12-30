var thrift = require('node-thrift')
  , ttransport = require('node-thrift/lib/thrift/transport')
  , TCLIService = require('./lib/shib/engines/hiveserver2/TCLIService')
  , TTypes = require('./lib/shib/engines/hiveserver2/TCLIService_types');

var MaxRows = 10000;

var fetchOperationResult = function(op, err, res, callback) {
  console.log({seq:'fetchOperationResult', op:op, err:err, res:res});
  if (err) { callback(err); return; }
  if (!res || !res['status'] || res.status['statusCode'] !== 0) {
    callback({err:'Failed to operate "' + op + '"', res:res, status: (res && res['status'] && res.status['statusCode'])});
    return;
  }
  var oph = res.operationHandle;
  var POLLING_INTERVAL = 50; // 50ms
  var poller = function(){
    console.log({seq:'GetOperationStatus', op:op});
    client.GetOperationStatus(new TTypes.TGetOperationStatusReq({operationHandle:oph}), function(err, res){
      console.log({seq:'GetOperationStatus', op:op, err:err, res:res});
      if (err) { callback(err); return; }
      var statusCode = res && res['status'] && res.status.statusCode;
      if (statusCode === TTypes.TStatusCode['STILL_EXECUTING_STATUS']) {
        setTimeout(poller, POLLING_INTERVAL);
        return;
      }
      if (statusCode !== TTypes.TStatusCode['SUCCESS_STATUS'] &&
          statusCode !== TTypes.TStatusCode['STATUS_WITH_INFO_STATUS']) {
        if (statusCode === TTypes.TStatusCode['ERROR_STATUS']) {
          callback({err:'ERROR_STATUS', status:res.status});
        } else if (statusCode === TTypes.TStatusCode['INVALID_HANDLE_STATUS']) {
          callback({err:'INVALID_HANDLE_STATUS', status:res.status});
        } else {
          callback({err:'UNKNOWN STATUS:' + statusCode, status:res.status});
        }
        client.CloseOperation(new TTypes.TCloseOperationReq({operationHandle:oph}), function(err, res){
          return err || res && res['status'];
        });
      }

      /* success */
      if (res.status.statusCode === TTypes.TStatusCode['STATUS_WITH_INFO_STATUS']) {
        console.log({req:'withInfoStatus', res:res}); // TODO: what is WITH_INFO_STATUS ?
      }
      client.FetchResults(new TTypes.TFetchResultsReq({operationHandle:oph, maxRows:MaxRows}), function(err, res){
        if (err) { callback(err); return; }
        console.log({seq:'FetchResults', op:op, err:err, res:res});
        if (res.hasMoreRows)
          throw "Operation hasMoreRow, type:" + oph.operationType;
        var results = res.results;
        client.CloseOperation(new TTypes.TCloseOperationReq({operationHandle:oph}), function(err, res){
          callback(null, results);
          return;
        });
      });
    });
  };
  setTimeout(poller, POLLING_INTERVAL);
};

/* hive.server2.authentication='NOSASL' or SASLTransport ... */
var connection = thrift.createConnection("server.name.local", 10001, {transport: ttransport.TBufferedTransport, timeout: 600*1000});
var client = thrift.createClient(TCLIService, connection);

connection.on('error', function(err){ console.error(err); });
connection.addListener("connect", function(){ console.log("connected"); });

client.OpenSession(new TTypes.TOpenSessionReq({username: '', password: ''}), function(err,res){
  var sessionHandle = res.sessionHandle;
  var treq = new TTypes.TGetTablesReq({sessionHandle:sessionHandle});
  client.GetTables(treq, function(e,r){ fetchOperationResult('GetTables', e, r, function(err, res){
    console.log([err, res]);
    console.log(JSON.stringify(res.rows, null, 4));
    var sreq = new TTypes.TGetSchemasReq({sessionHandle:sessionHandle});
    client.GetSchemas(sreq, function(e,r){ fetchOperationResult('GetSchemas', e, r, function(err, res){
      console.log([err, res]);
      console.log(JSON.stringify(res.rows, null, 4));
      var creq = new TTypes.TCloseSessionReq({sessionHandle:sessionHandle});
      client.CloseSession(creq, function(e,r){ connection.end(); });
    }); });
  }); });
});

/*
http://www.slideshare.net/schubertzhang/hiveserver2/7

ExecuteStatement
  TExecuteStatementReq(sessionHandle, statement, confOverlay)
  TExecuteStatementResp(status, operationHandle)

GetResultSetMetadata
  TGetResultSetMetadataReq(operationHandle)
  TGetResultSetMetadataResp(status, schema)

FetchResults
  TFetchResultsReq(operationHandle, orientation = 0, maxRows)
  TFetchResultsResp(status, hasMoreRows, results)

GetCatalogs : "Catalog: do nothing so far"
  TGetCatalogsReq(sessionHandle)
  TGetCatalogsResp(status, operationHandle)

GetSchemas
  TGetSchemasReq(sessionHandle, catalogName, schemaName)
  TGetSchemasResp(status, operationHandle)

GetTableTypes
  TGetTableTypesReq(sessionHandle)
  TGetTableTypesResp(status, operationHandle)

GetTables
  TGetTablesReq(sessionHandle, catalogName, schemaName, tableName, tableTypes)
  TGetTablesResp(status, operationHandle)

GetColumns
  TGetColumnsReq(sessionHandle, catalogName, schemaName, tableName, columnName)
  TGetColumnsResp(status, operationHandle)

GetFunctions
  TGetFunctionsReq(sessionHandle, catalogName, schemaName, functionName)
  TGetFunctionsResp(status, operationHandle)

OpenSession
  TOpenSessionReq(username, password, configuration)
  TOpenSessionResp(status, serverProtocol, sessionHandle, configuration)
CloseSession
  TCloseSessionReq(sessionHandle)

GetInfo
  TGetInfoReq(sessionHandle, infoType)
  TGetInfoResp(status, infoValue)
GetTypeInfo
  TGetTypeInfoReq(sessionHandle)
  TGetTypeInfoResp(status, operationHandle)

GetOperationStatus
  TGetOperationStatusReq(operationHandle)
  TGetOperationStatusResp(status, operationState)
GetCancelOperation
  TCancelOperationReq(operationHandle)
  TCancelOperationResp(status)
CloseOperation
  TCloseOperationReq(operationHandle)
  TCloseOperationResp(status)


オマケ: SessionHandle と OperationHandle
TSessionHandle(sessionId)
TOperationHandle(operationId, operationType, hasResultSet, modifiedRowCount) 
 */