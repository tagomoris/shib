var async = require('async')
  , fs = require('fs')
  , sqlite3 = require('sqlite3');

var target_db_path = process.argv[2];
if (! target_db_path) {
  console.log("USAGE: npm run migrate");
  console.log("   OR: npm run dbmigrate -- DATABASE_FILE_PATH");
  console.log("     : node bin/dbmigrate.js DATABASE_FILE_PATH (if your npm --version is 1.x)");
  process.exit(0);
}
var migrate_db_path = target_db_path + ".migrate";

/*
var SQLITE_TABLE_DEFINITIONS_v0 = [
  'CREATE TABLE IF NOT EXISTS queries (id VARCHAR(32) NOT NULL PRIMARY KEY, json TEXT NOT NULL)',
  'CREATE TABLE IF NOT EXISTS results (id VARCHAR(32) NOT NULL PRIMARY KEY, json TEXT NOT NULL)',
  'CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, yyyymm VARCHAR(6) NOT NULL, queryid VARCHAR(32) NOT NULL)',
  'CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY AUTOINCREMENT, queryid VARCHAR(32) NOT NULL, tag VARCHAR(16) NOT NULL)'
];
*/

var SQLITE_TABLE_DEFINITIONS_v1 = [
  'CREATE TABLE IF NOT EXISTS queries (autoid INTEGER PRIMARY KEY AUTOINCREMENT, id VARCHAR(32) NOT NULL UNIQUE, datetime TEXT NOT NULL, scheduled INTEGER DEFAULT NULL, engine TEXT DEFAULT NULL, dbname DEFAULT NULL, expression TEXT NOT NULL, state VARCHAR(32) NOT NULL, resultid VARCHAR(32) NOT NULL UNIQUE, result DEFAULT NULL)',
  'CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY AUTOINCREMENT, queryid VARCHAR(32) NOT NULL, tag VARCHAR(16) NOT NULL)'
];

var on_connect = function(cb){
  async.series(SQLITE_TABLE_DEFINITIONS_v1.map(function(sql){
	return function(callback) {
      migrate.run(sql, function(error){
        if (error) { callback(error.message); return; }
        callback(null);
      });
	};
  }), function(err,results){
	if (err)
      throw "failed to initialize new db file: " + migrate_db_path;
    cb();
  });
};

var original;
var migrate;

var open_original = function(cb){
  original = new sqlite3.Database(target_db_path, sqlite3.OPEN_READONLY, function(err){
    if (err) { cb(err); return; }
    cb(null);
  });
};

var close_original = function(cb){
  original.close(function(){ cb(null); });
};

var open_migrate = function(cb){
  migrate = new sqlite3.Database(migrate_db_path, (sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE), function(err){
    if (err) { cb(err); return; }
    on_connect(function(){ cb(null); });
  });
};

var close_migrate = function(cb){
  migrate.close(function(){ cb(null); });
};

var migrate_tags = function(cb){
  original.all('SELECT queryid, tag FROM tags ORDER BY id', function(err, rows){
    if (err) {
      cb(err);
      return;
    }
    async.series(rows.map(function(row){
      return function(cb){
        migrate.run('INSERT INTO tags (queryid, tag) VALUES (?,?)', [row.queryid, row.tag], function(err){
          cb(err);
        });
      };
    }), function(err, results){
      cb(err);
    });
  });
};

var history = {}; // queryid => true

var store_history = function(cb){
  // 'CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, yyyymm VARCHAR(6) NOT NULL, queryid VARCHAR(32) NOT NULL)',
  original.all('SELECT yyyymm, queryid FROM history', function(err, rows){
    if (err) { cb(err); return; }
    var size = rows.length - 1;
    for (var i = 0 ; i < size ; i++){
      history[rows[i].queryid] = true;
    }
    cb(null);
  });
};

var results = {}; // resultid -> obj

var store_results = function(cb){
  original.all('SELECT id, json FROM results', function(err, rows){
    if (err) { cb(err); return; }
    var size = rows.length - 1;
    var row = null;
    for (var i = 0 ; i < size ; i++){
      row = rows[i];
      results[row.id] = row.json;
    };
    cb(null);
  });
};

var migrate_queries = function(cb){
  original.all('SELECT id, json FROM queries', function(err, rows_original){
    if (err) {
      cb(err);
      return;
    }
    var rows = rows_original.filter(function(row){
      var json = row.json;
      if (json) {
        var r = JSON.parse(json).results.concat().pop();
        if (r && results[r.resultid])
          return true;
      }
      return false;
    }).map(function(row){
      var obj = JSON.parse(row.json);
      var result = obj.results.pop();
      var result_obj = JSON.parse(results[result.resultid]);
      var state = result_obj['state'];
      delete result_obj['state'];
      delete result_obj['queryid'];
      delete result_obj['resultid'];
      return {
        id: row.id,
        scheduled: (history[row.id] ? 1 : null),
        engine: obj.engine,
        dbname: obj.dbname,
        expression: obj.querystring,
        state: state,
        resultid: result.resultid,
        result_json: JSON.stringify(result_obj),
        date: new Date(result.executed_at).toJSON()
      };
    });
    rows.sort(function(a, b){ return a.date - b.date; });
    async.series(rows.map(function(obj){
      return function(cb) {
        migrate.run(
            'INSERT INTO queries (id,datetime,scheduled,engine,dbname,expression,state,resultid,result) VALUES (?,?,?,?,?,?,?,?,?)',
            [obj.id, obj.date, obj.scheduled, obj.engine, obj.dbname, obj.expression, obj.state, obj.resultid, obj.result_json],
            function(err){ cb(err); }
        );
      };
    }), function(err, results){ cb(err); });
  });
};

async.series([
  open_original,
  open_migrate,
  migrate_tags,
  store_history,
  store_results,
  migrate_queries,
  close_original,
  close_migrate
], function(err, results){
  if (err) {
    console.log(err);
    throw "failed to migrate database...";
  }
  fs.renameSync(target_db_path, target_db_path + ".v0");
  fs.renameSync(migrate_db_path, target_db_path);
});
