var fs = require('fs'),
    async = require('async'),
    sqlite3 = require('sqlite3');

var Query = require('./query').Query,
    Result = require('./result').Result;

var DATABASE_SQLITE_FILENAME = 'database.sqlite3';

// results raw data -> ${datadir}/results/char[0]/char[1]/resultid

// queries/results/history -> ${datadir}/database
//  [queries] id -> json
//  [results] id -> json
//  [history] id, datetime, queryid
/*
 CREATE TABLE history (
   id INTEGER PRIMARY KEY AUTOINCREMENT,
   yyyymm CHAR(6) TEXT,
   data TEXT
 ) 
 */
var SQLITE_TABLE_DEFINITIONS = [
  'CREATE TABLE IF NOT EXISTS queries (id VARCHAR(32) NOT NULL PRIMARY KEY, json TEXT NOT NULL)',
  'CREATE TABLE IF NOT EXISTS results (id VARCHAR(32) NOT NULL PRIMARY KEY, json TEXT NOT NULL)',
  'CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, yyyymm VARCHAR(6) NOT NULL, queryid VARCHAR(32) NOT NULL)'
];

var LocalDiskStoreError = exports.LocalDiskStoreError = function(msg){
  this.name = 'LocalDiskStoreError';
  Error.call(this, msg);
  Error.captureStackTrace(this, arguments.callee);
};
LocalDiskStoreError.prototype.__proto__ = Error.prototype;

var LocalDiskStore = function(datadir) {
  this.datadir = fs.realpathSync(datadir);
  if (! this.datadir )
    throw new LocalDiskStoreError("Invalid datadir path: " + datadir);

  var stat = fs.statSync(this.datadir);
  if (! stat) {
    fs.mkdirSync(this.datadir);
  } else if (! stat.isDirectory()) {
    throw new LocalDiskStoreError("Specified path is not directory: " + datadir);
  }

  var db;
  var on_connect = function(error){
    if (error) { console.log(error); throw new LocalDiskStoreError("sqlite3 database open error:" + error.message); }
    if (!db) { console.log('db initialize error'); throw new LocalDiskStoreError("db variable is unset, maybe not initialized"); }
    async.series(SQLITE_TABLE_DEFINITIONS.map(function(sql){
      return function(callback) {
        db.run(sql, function(error){
          if (error) { callback(error.message); return; }
          callback(null);
        });
      };
    }), function(err,results){
      if (err) {
        console.log('database initialize failed');
        console.log(err);
        throw new LocalDiskStoreError("database initialize failed");
      }
    });
  };
  db = this.db = new sqlite3.Database(
    datadir + '/' + DATABASE_SQLITE_FILENAME,
    (sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE),
    on_connect
  );
};

LocalDiskStore.prototype.close = function(){
  this.db.close();
};

// operations with sqlite3

function generatePlaceholders(num) {
  if (num < 1)
    throw "placeholder num must bigger than zero";
  var p = '?';
  for (var i = 1; i < num; i++) {
    p = p.concat(',?');
  }
  return p;
}

// 'CREATE TABLE IF NOT EXISTS queries (id VARCHAR(32) NOT NULL PRIMARY KEY, json TEXT NOT NULL)',
LocalDiskStore.prototype.query = function(queryid, callback){
  this.db.get('SELECT json FROM queries WHERE id=?', [queryid], function(err,row){
    if (err) { callback(err); return; }
    if (!row) { callback(null, null); return; }
    callback(null, new Query({json:row.json}));
  });
};
LocalDiskStore.prototype.queries = function(queryids, callback){
  var placeholders = '(' + generatePlaceholders(queryids.length) + ')';
  this.db.all('SELECT json FROM queries WHERE id IN ' + placeholders, queryids, function(err,rows){
    if (err) { callback(err); return; }
    callback(null, rows.map(function(r){ return new Query({json:r.json}); }));
  });
};
LocalDiskStore.prototype.insertQuery = function(query, callback){
  this.db.run('INSERT INTO queries (id,json) VALUES (?,?)', [query.queryid, query.serialized()], function(err){
    if (callback) callback(err);
  });
};
LocalDiskStore.prototype.updateQuery = function(query, callback){
  this.db.run('UPDATE queries SET json=? WHERE id=?', [query.serialized(), query.queryid], function(err){
    if (callback) callback(err);
  });
};
LocalDiskStore.prototype.deleteQuery = function(queryid, callback){
  this.db.run('DELETE FROM queries WHERE id=?', [queryid], function(err){
    if (callback) callback(err);
  });
};

// 'CREATE TABLE IF NOT EXISTS results (id VARCHAR(32) NOT NULL PRIMARY KEY, json TEXT NOT NULL)',
LocalDiskStore.prototype.result = function(resultid, callback){
  this.db.get('SELECT json FROM results WHERE id=?', [resultid], function(err,row){
    if (err) { callback(err); return; }
    if (!row) { callback(null, null); return; }
    callback(null, new Result({json:row.json}));
  });
};
LocalDiskStore.prototype.results = function(resultids, callback){
  var placeholders = '(' + generatePlaceholders(resultids.length) + ')';
  this.db.all('SELECT json FROM results WHERE id IN ' + placeholders, resultids, function(err,rows){
    if (err) { callback(err); return; }
    callback(null, rows.map(function(r){ return new Result({json:r.json}); }));
  });
};
LocalDiskStore.prototype.insertResult = function(result, callback){
  this.db.run('INSERT INTO results (id,json) VALUES (?,?)', [result.resultid, result.serialized()], function(err){
    if (callback) callback(err);
  });
};
LocalDiskStore.prototype.updateResult = function(result, callback){
  this.db.run('UPDATE results SET json=? WHERE id=?', [result.serialized(), result.resultid], function(err){
    if (callback) callback(err);
  });
};
LocalDiskStore.prototype.deleteResult = function(resultid, callback){
  this.db.run('DELETE FROM results WHERE id=?', [resultid], function(err){
    if (callback) callback(err);
  });
};

// 'CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, yyyymm VARCHAR(6) NOT NULL, queryid VARCHAR(32) NOT NULL)'
LocalDiskStore.prototype.recentQueries = function(num, callback){
  var sql = 'SELECT yyyymm,queryid FROM history ORDER BY id DESC LIMIT ' + parseInt(num);
  var db = this.db;
  this.db.all(sql, function(err, rows){
    if (err) { callback(err); return; }
    callback(null, rows);
  });
};
LocalDiskStore.prototype.addRecent = function(yyyymm, queryid, callback){
  this.db.run('INSERT INTO history (yyyy,queryid) VALUES (?,?)', [yyyymm, queryid], function(err){
    if (callback) callback(err);
  });
};

/* result data path utilities */
LocalDiskStore.prototype.generatePathElements = function(key){
  return [this.datadir, 'results', key[0], key[1], key];
};
LocalDiskStore.prototype.generatePath = function(key){
  return this.generatePathElements(key).join("/");
};
LocalDiskStore.prototype.keyExists = function(key){
  return fs.existsSync(this.generatePath(key));
};
LocalDiskStore.prototype.prepare = function(key, callback){
  var elements = this.generatePathElements(key);
  var depth = elements.length - 1;

  var dig = function(elements, depth, cb){
    var path = elements.slice(0,depth);
    fs.exists(path, function(exists){
      if (!exists) {
        fs.mkdir(path, function(){ cb(depth); });
        return;
      }
      cb(depth);
    });
  };
  var dig_callback = function(digged){
    if (digged < elements.length) {
      dig(elements, digged + 1, dig_callback);
      return;
    }
    callback();
  };
  dig(elements, 1, dig_callback);
};
LocalDiskStore.prototype.readResultData = function(key, callback){
  if (! this.keyExists(key)) { callback(null,null); return; }

  var path = this.generatePath(key);
  fs.readFile(path, 'utf8', function(err, data){
    callback(err, data);
  });
};
LocalDiskStore.prototype.writeResultData = function(key, data, callback){
  if (this.keyExists(key)) {
    console.log("specified result data to write is already exists, key:" + key);
    callback({message:"specified result data to write is already exists, key:" + key});
    return;
  }

  var path = this.generatePath(key);
  fs.writeFile(path, data, 'utf8', function(err){
    callback(err);
  });
};
LocalDiskStore.prototype.appendResultData = function(key, data, callback){
  var path = this.generatePath(key);
  fs.appendFile(path, data, 'utf8', function(err){
    callback(err);
  });
};
