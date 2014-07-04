var async = require('async');
var Executer = require('shib/engines/hiveserver2').Executer;

var host = ''
  , port = 10000
  , username = ''
  , password = '';

var test_database = ''
  , test_table = '';

var use_database_statement = "use " + test_database;

var executer = new Executer({
  name: 'hiveserver2',
  host: host,
  port: port,
  username: username,
  password: password
});

// Executer.prototype.execute = function(jobname, dbname, query, callback)

async.series([
  function(cb){
    executer.execute(null, null, "show databases", function(err, fetcher){
      fetcher.fetch(null, function(err, result){
        console.log({label:"RESULT: show databases", result:result});
        cb();
      });
    });
  },
  function(cb){
    executer.setup([use_database_statement], function(err){
      executer.execute(null, null, "show tables", function(err, fetcher){
        fetcher.fetch(null, function(err, result){
          console.log({label:"RESULT show tables", result:result});
          cb();
        });
      });
    });
  },
  function(cb){
    executer.setup([use_database_statement], function(err){
      executer.execute(null, null, "describe " + test_table, function(err, fetcher){
        fetcher.fetch(null, function(err, result){
          console.log({label:"RESULT describe", result:result});
          cb();
        });
      });
    });
  },
  function(cb){
    executer.setup([use_database_statement], function(err){
      executer.execute(null, null, "show partitions " + test_table, function(err, fetcher){
        fetcher.fetch(null, function(err, result){
          console.log({label:"RESULT show partitions", result:result});
          cb();
        });
      });
    });
  }
], function(err,results){ executer.end(); console.log("END."); });
