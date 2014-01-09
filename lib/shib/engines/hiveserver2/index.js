var thrift = require('node-thrift')
  , ttransport = require('node-thrift/lib/thrift/transport')
  , ThriftHive = require('./TCLIService')
  , TTypes = require('./TCLIService_types');

var MaxRows = 100000;
var OperationStatusPollingInterval = 200; // 200ms

var runningOperations = {}; // For Monitor methods

var Executer = exports.Executer = function(conf){
  if (conf.name !== 'hiveserver2')
    throw "executer name mismatch for hiveserver2:" + conf.name;

  /* hive.server2.authentication='NOSASL' or SASLTransport ... */
  this._connection = thrift.createConnection(
    conf.host,
    conf.port,
    {transport: ttransport.TBufferedTransport}
  );
  this._client = thrift.createClient(ThriftHive, this._connection);
  this._sessionHandle = null;
  this._maxRows = conf['maxRows'] || MaxRows;
};

Executer.prototype._inSession = function(callback){
  if (this._sessionHandle) {
    callback(null); return;
  }
  var self = this;
  var openSessionReq = new TTypes.TOpenSessionReq({username: '', password: ''});
  this._client.OpenSession(openSessionReq, function(err, res){
    if (err) { callback(err); return; }
    if (! res.sessionHandle){ callback({message: "sessionHandle missing without any reason"}); return; }
    self._sessionHandle = res.sessionHandle;
    callback(null);
  });
};

Executer.prototype.end = function(){
  if (this._client) {
    var self = this;
    if (! this._sessionHandle) {
      this._connection.end();
      return;
    }
    var closeReq = new TTypes.TCloseSessionReq({sessionHandle: this._sessionHandle});
    this._client.CloseSession(closeReq, function(){ self._connection.end(); });
  }
};

Executer.prototype.supports = function(operation){
  switch (operation) {
  case 'setup':
  case 'execute':
    return true;
  }
  throw "unknown operation name (for hiveserver2.Executer):" + operation;
};

Executer.prototype.setup = function(setups, callback){
  if (!setups || setups.length < 1) {
    callback(null); return;
  }

  var self = this;
  var client = this._client;
  var setupQueue = setups.concat(); // shallow copy of Array to use as queue

  var executeSetup = function(queue, callback){
    var q = queue.shift();
    self.execute(null, q, function(err, fetcher){
      if (err)
        callback(err);
      else
        fetcher.fetch(null,function(err,rows){
          if (err) { callback(err); return; }
          if (queue.length > 0)
            executeSetup(queue, callback);
          else
            callback(null);
        });
    });
  };

  this._inSession(function(err){
    if (err){ callback(err); return; }
    executeSetup(setupQueue, callback);
  });
};

Executer.prototype.execute = function(jobname, query, callback){
  var self = this;
  var client = this._client;

  /* confOverlay argument of TExecuteStatementReq doesn't works with 'mapred.job.name' and 'mapreduce.job.name' ... */
  var setJobName = [];
  if (jobname && jobname !== '') {
    setJobName.push('set mapred.job.name=' + jobname);
    setJobName.push('set mapreduce.job.name=' + jobname);
  }
  this._inSession(function(err){
    if (err) { callback(err); return; }
    self.setup(setJobName, function(err){
      if (err) { callback(err); return; }
      var executeReq = new TTypes.TExecuteStatementReq({sessionHandle:self._sessionHandle, statement:query});
      client.ExecuteStatement(executeReq, function(err, res){
        if (err) { callback(err); return; }
        /*
ttypes.TStatusCode = {
'SUCCESS_STATUS' : 0,
'SUCCESS_WITH_INFO_STATUS' : 1,
'STILL_EXECUTING_STATUS' : 2,
'ERROR_STATUS' : 3,
'INVALID_HANDLE_STATUS' : 4
};
ttypes.TOperationState = {
'INITIALIZED_STATE' : 0,
'RUNNING_STATE' : 1,
'FINISHED_STATE' : 2,
'CANCELED_STATE' : 3,
'CLOSED_STATE' : 4,
'ERROR_STATE' : 5,
'UKNOWN_STATE' : 6
};
         */
        //TODO: fix statusCode to check
        if (!res || !res['status'] || res.status['statusCode'] !== 0) {
          callback({message:"Failed to execute statement", res:res, status:(res && res['status'] && res.status['statusCode'])});
          return;
        }
        if (! res['operationHandle']) {
          callback({message:"operationHandle missing for query", res:res, status:(res && res['status'] && res.status['statusCode'])});
          return;
        }
        // fetch operation fesult
        callback(null, new Fetcher(client, res.operationHandle, self._maxRows));
      });
    });
  });
};

var Fetcher = function(client, operationHandle, maxRows){
  this._client = client;
  this._oph = operationHandle;
  this._opStatus = null;
  this._noMoreResults = false;
  this._maxRows = maxRows;
  this._fetching = false;

  this.schema = function(callback){
    var req = new TTypes.TGetResultSetMetadataReq({operationHandle: this._oph});
    this._client.GetResultSetMetadata(req, function(err,res){
      if (err) { callback(err); return; }
/*
 * schema(callback): callback(err, schema)
 *  schema: {fieldSchemas: [{name:'fieldname1'}, {name:'fieldname2'}, {name:'fieldname3'}, ...]}
 *  //?? schema: [{name:'fieldname1'}, {name:'fieldname2'}, ...]
 */
      //TODO: res.status ?
      //TODO: column types to read result of fetchResults ?
      var result = {
        fieldSchemas: res.schema.columns.map(function(c){return {name:c.columnName};})
      };
      callback(null, result);
/*
ttypes.TTypeId = {
'BOOLEAN_TYPE' : 0,
'TINYINT_TYPE' : 1,
'SMALLINT_TYPE' : 2,
'INT_TYPE' : 3,
'BIGINT_TYPE' : 4,
'FLOAT_TYPE' : 5,
'DOUBLE_TYPE' : 6,
'STRING_TYPE' : 7,
'TIMESTAMP_TYPE' : 8,
'BINARY_TYPE' : 9,
'ARRAY_TYPE' : 10,
'MAP_TYPE' : 11,
'STRUCT_TYPE' : 12,
'UNION_TYPE' : 13,
'USER_DEFINED_TYPE' : 14,
'DECIMAL_TYPE' : 15
};
 */
/* res.schema
{
  "columns": [
    {
      "columnName": "service",
      "typeDesc": {
        "types": [
          {
            "primitiveEntry": {
              "type": 7
            },
            "arrayEntry": null,
            "mapEntry": null,
            "structEntry": null,
            "unionEntry": null,
            "userDefinedTypeEntry": null
          }
        ]
      },
      "position": 1,
      "comment": null
    },
    {
      "columnName": "cnt",
      "typeDesc": {
        "types": [
          {
            "primitiveEntry": {
              "type": 4
            },
            "arrayEntry": null,
            "mapEntry": null,
            "structEntry": null,
            "unionEntry": null,
            "userDefinedTypeEntry": null
          }
        ]
      },
      "position": 2,
      "comment": null
    }
  ]
}
 */

    });
  };

  // stringify value of each columns
  this._colValue = function(obj) {
    /*
    colVals = [
      {
        "boolVal": null, "byteVal": null, "i16Val": null, "i32Val": null, "i64Val": null, "doubleVal": null,
        "stringVal": { "value": "blog" }
      },
      {
        "boolVal": null, "byteVal": null, "i16Val": null, "i32Val": null,
        "i64Val": {
          "value": { "buffer": [0, 0, 0, 0, 63, 218, 72, 34], "offset": 0 }
        },
        "doubleVal": null, "stringVal": null
      },
      {
        "boolVal": { "value": false },
        "byteVal": null, "i16Val": null, "i32Val": null, "i64Val": null, "doubleVal": null, "stringVal": null
      },
      {
        "boolVal": null, "byteVal": null, "i16Val": null, "i32Val": null, "i64Val": null,
        "doubleVal": { "value": 0.01 },
        "stringVal": null
      },
      {
        "boolVal": null, "byteVal": null, "i16Val": null,
        "i32Val": { "value": 1 },
        "i64Val": null, "doubleVal": null, "stringVal": null
      },
      {
        "boolVal": null, "byteVal": null, "i16Val": null, "i32Val": null, "i64Val": null, "doubleVal": null,
        "stringVal": {
          "value": "[1, 2, 3]"
        }
      }
    ]
     */
    if (obj['stringVal'] !== null)
      return obj.stringVal.value;

    if (obj['boolVal'] !== null)
      return String(obj.boolVal.value).toUpperCase();

    // rest are numerics, or NULL
    // byteVal, i16Val, i32Val, i64Val, doubleVal
    var value;
    if (obj['i64Val'] !== null)
      value = obj.i64Val.value;
    else if (obj['i32Val'] !== null)
      value = obj.i32Val.value;
    else if (obj['i16Val'] !== null)
      value = obj.i16Val.value;
    else if (obj['doubleVal'] !== null)
      value = obj.doubleVal.value;
    else if (obj['byteVal'] !== null)
      value = obj.byteVal.value;
    else
      return 'NULL';

    if (value['buffer'] !== undefined) {
      var shifts = 1;
      var buffer = value.buffer;
      var v = 0;
      // shift operator doesn't works for nums larger than 32bit
      for (var i = buffer.length - 1, s = 1; i >= 0; i--, s*= 256) {
        v += buffer[i] * s;
      }
    }

    // direct value
    return value;
  };

  this.fetch = function(num, callback){
    if (!num) {
      this._fetchAll(callback);
      return;
    }

    if (this._noMoreResults) {
      callback(null, null);
      return;
    }

    var self = this;
    this._waitComplete(function(err){
      if (err) {
        callback(err);
        self._closeOperation();
        return;
      }
      /*
       ttypes.TFetchOrientation = {
         'FETCH_NEXT' : 0,
         'FETCH_PRIOR' : 1,
         'FETCH_RELATIVE' : 2,
         'FETCH_ABSOLUTE' : 3,
         'FETCH_FIRST' : 4,
         'FETCH_LAST' : 5
       };
      */
      var orientation = TTypes.TFetchOrientation['FETCH_NEXT'];
      var req = new TTypes.TFetchResultsReq({operationHandle:self._oph, orientation:orientation, maxRows:num});
      self._client.FetchResults(req, function(err, res){
        if (err) {
          if (res['status'])
            console.log(JSON.stringify(res.status, null, " "));
          callback(err);
          self._closeOperation();
          return;
        }
        //TODO: res.status for successes
        /*
        "status": {
          "statusCode": 0,
          "infoMessages": null,
          "sqlState": null,
          "errorCode": null,
          "errorMessage": null
        }
         */
        if (! res.hasMoreRow) {
          self._noMoreResults = true;
          self._closeOperation();
        }

        var rows = [];
        var fetchedRows = res.results.rows;
        var fetchedLength = fetchedRows.length;
        for (var i = 0; i < fetchedLength; i++) {
          var cols = [];
          var colVals = fetchedRows[i].colVals;
          var colValsLength = colVals.length;
          for (var j = 0; j < colValsLength; j++) {
            cols.push(self._colValue(colVals[j]));
          }
          rows.push(cols.join("\t"));
        }
/*
 *  rows: ["_col1value_\t_col2value_\t_col3value_", "_col1value_\t_col2value_\t_col3value_", ...]
 *    no more rows exists if (rows === null || rows.length < 1 || (rows.length == 1 && rows[0].length < 1))
 */
        callback(null, rows);
/* res.results
[
  {
    "startRowOffset": {
      "buffer": [
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0
      ],
      "offset": 0
    },
    "rows": [
      {
        "colVals": [
          {
            "boolVal": null,
            "byteVal": null,
            "i16Val": null,
            "i32Val": null,
            "i64Val": null,
            "doubleVal": null,
            "stringVal": {
              "value": "blog"
            }
          },
          {
            "boolVal": null,
            "byteVal": null,
            "i16Val": null,
            "i32Val": null,
            "i64Val": {
              "value": {
                "buffer": [
                  0,
                  0,
                  0,
                  0,
                  63,
                  218,
                  72,
                  34
                ],
                "offset": 0
              }
            },
            "doubleVal": null,
            "stringVal": null
          }
        ]
      },
      {
        "colVals": [
          {
            "boolVal": null,
            "byteVal": null,
            "i16Val": null,
            "i32Val": null,
            "i64Val": null,
            "doubleVal": null,
            "stringVal": {
              "value": "news"
            }
          },
          {
            "boolVal": null,
            "byteVal": null,
            "i16Val": null,
            "i32Val": null,
            "i64Val": {
              "value": {
                "buffer": [
                  0,
                  0,
                  0,
                  0,
                  5,
                  132,
                  254,
                  253
                ],
                "offset": 0
              }
            },
            "doubleVal": null,
            "stringVal": null
          }
        ]
      }
    ],
    "columns": null
  }
]
 */
      });
    });
  };

  this._fetchAll = function(callback){
    var r = [];
    var self = this;
    var fetcher = function(){
      self.fetch(self._maxRows, function(err, results){ //TODO: results ? status ?
        if (err) { callback(err); return; }
        if (results === null || results.length < 1 || results.length === 1 && results[0].length < 1) {
          callback(null, r);
          return;
        }
        r = r.concat(results);
        fetcher();
      });
    };
    fetcher();
  };

  this._closeOperation = function(callback){
    var req = new TTypes.TCloseOperationReq({operationHandle: this._oph});
    client.CloseOperation(req, function(err, res){
      if (callback) {
        callback();
      }
    });
  };

  this._waitComplete = function(callback){
    if (this._opStatus !== null) {
      callback(null);
      return;
    }

    var self = this;
    var oph = this._oph;
    var interval = OperationStatusPollingInterval;
    var client = this._client;
    var poller = function(){
      var req = new TTypes.TGetOperationStatusReq({operationHandle: oph});
      client.GetOperationStatus(req, function(err,res){
        if (err) { callback(err); return; }

        var statusCode = res && res['status'] && res.status.statusCode;
        if (statusCode === TTypes.TStatusCode['STILL_EXECUTING_STATUS']) {
          setTimeout(poller, interval);
          return;
        }

        self._opStatus = statusCode;

        if (statusCode !== TTypes.TStatusCode['SUCCESS_STATUS'] &&
            statusCode !== TTypes.TStatusCode['STATUS_WITH_INFO_STATUS']) {
          var error_name = 'UNKNOWN';
          if (statusCode === TTypes.TStatusCode['ERROR_STATUS']) {
            error_name = 'ERROR_STATUS';
          } else if (statusCode === TTypes.TStatusCode['INVALID_HANDLE_STATUS']) {
            error_name = 'INVALID_HANDLE_STATUS';
          }
          callback({message:'Statement failed with status:' + error_name + ' (' + String(statusCode) + ')', status:res.status});
        }
        /* success complete */
        callback(null);
      });
    };
    poller();
  };
};

// TODO: Monitor impl after interface fix
