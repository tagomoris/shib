var async = require('async')
  , fs = require('fs')
  , sqlite3 = require('sqlite3');

if (process.argv.length < 4) {
  console.log("USAGE: npm run purge -- DAYS_PURGE_BEFORE");
  console.log("   OR: node bin/purge.js DATABASE_FILE_PATH DAYS_PURGE_BEFORE (if your npm --version is 1.x)");
  console.log("");
  console.log("NOTICE: result data files under DATADIR/results should be removed by yourself.");
  console.log("        (ex: find var/results -mtime ... | xargs rm)");
  process.exit(0);
}
var target_db_path = process.argv[2];
var purge_days_before = parseInt(process.argv[3]);
if (purge_days_before < 3) {
  console.log("ERROR: DAYS_PURGE_BEFORE must be larger than 7, but: " + process.argv[3]);
  process.exit(1);
}
  
var SQLITE_TABLE_DEFINITIONS_v1 = [
  'CREATE TABLE IF NOT EXISTS queries (autoid INTEGER PRIMARY KEY AUTOINCREMENT, id VARCHAR(32) NOT NULL UNIQUE, datetime TEXT NOT NULL, engine TEXT DEFAULT NULL, dbname DEFAULT NULL, expression TEXT NOT NULL, state VARCHAR(32) NOT NULL, resultid VARCHAR(32) NOT NULL UNIQUE, result DEFAULT NULL)',
  'CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY AUTOINCREMENT, queryid VARCHAR(32) NOT NULL, tag VARCHAR(16) NOT NULL)'
];

var db = null;
var open_db = function(cb){
  db = new sqlite3.Database(target_db_path, (sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE), function(err){
    cb(err);
  });
};

var close_db = function(cb){
  db.close(function(){ cb(null); });
};

var delete_rows = function(cb){
  var purge_threshold_datetime = new Date(new Date() - purge_days_before * 1000 * 86400).toJSON();
  db.run('DELETE FROM queries WHERE datetime < ?', [purge_threshold_datetime], function(err){ cb(err); });
};

var vacuum = function(cb){
  db.run('VACUUM', function(err){ cb(err); });
};

async.series([
  open_db,
  delete_rows,
  vacuum,
  close_db
], function(err, results){
  if (err) {
    console.log(err);
    throw "failed to purge old data and vacuum...";
  }
});

