var async = require('async');
var Executer = require('shib/engines/hiveserver2/test').Executer;

var host = ''
  , port = 10000
  , username = ''
  , password = '';

var test_database = ''
  , test_table = '';

var executer = new Executer({
  host: host,
  port: port,
  username: username,
  password: password
});

// Executer.prototype.execute = function(jobname, dbname, query, callback)

async.series([
  function(cb){
    executer.databases(function(err, result){
      console.log({label:"RESULT: databases()", result:result});
      cb();
    });
  },
  function(cb){
    executer.tables(test_database, function(err, result){
      console.log({label:"RESULT tables()", result:result});
      cb();
    });
  },
  function(cb){
    executer.describe(test_database, test_table, function(err, result){
      console.log({label:"RESULT describe()", result:result});
      cb();
    });
  },
  function(cb){
    executer.partitions2(test_database, test_table, function(err, result){
      console.log({label:"RESULT partitions2()", result:result});
      cb();
    });
  },
  function(cb){
    executer.partitions(test_database, test_table, function(err, result){
      console.log({label:"RESULT partitions()", result:result});
      cb();
    });
  }
], function(err,results){ executer.end(); console.log("END."); });
