var fs = require('fs'),
    async = require('async'),
    sqlite3 = require('sqlite3');

var Query = require('./query').Query,
    Result = require('./result').Result;

var DATABASE_SQLITE_FILENAME = 'database.sqlite3';

/*
 * v0 data schema:
 * * results raw data -> ${datadir}/results/char[0]/char[1]/resultid
 * queries/results/history/tags -> ${datadir}/database
 *  [queries] id -> json
 *  [results] id -> json
 *  [history] id, datetime, queryid
 *  [tags]    id, queryid, tag
 */
var SQLITE_TABLE_DEFINITIONS_v0 = [
  'CREATE TABLE IF NOT EXISTS queries (id VARCHAR(32) NOT NULL PRIMARY KEY, json TEXT NOT NULL)',
  'CREATE TABLE IF NOT EXISTS results (id VARCHAR(32) NOT NULL PRIMARY KEY, json TEXT NOT NULL)',
  'CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, yyyymm VARCHAR(6) NOT NULL, queryid VARCHAR(32) NOT NULL)',
  'CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY AUTOINCREMENT, queryid VARCHAR(32) NOT NULL, tag VARCHAR(16) NOT NULL)'
];

/*
 * v1 data schema:
 * * results raw data -> ${datadir}/results/char[0]/char[1]/resultid
 * queries/tags -> ${datadir}/database
 * [queries] id -> auto_increment_id, datetime(new Date().toJSON(), Zulu), expression, state, resultid, result(JSON)
 */
var SQLITE_TABLE_DEFINITIONS = [
  'CREATE TABLE IF NOT EXISTS queries (autoid INTEGER PRIMARY KEY AUTOINCREMENT, id VARCHAR(32) NOT NULL UNIQUE, datetime TEXT NOT NULL, engine TEXT DEFAULT NULL, dbname DEFAULT NULL, expression TEXT NOT NULL, state VARCHAR(32) NOT NULL, resultid VARCHAR(32) DEFAULT NULL, result DEFAULT NULL)',
  'CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY AUTOINCREMENT, queryid VARCHAR(32) NOT NULL, tag VARCHAR(16) NOT NULL)'
];

var KNOWN_TABLES = ['queries', 'tags', 'sqlite_sequence'];
/*
 ** "SELECT name FROM sqlite_master WHERE type='table'"
 [ { name: 'queries' },
   { name: 'sqlite_sequence' },
   { name: 'tags' } ]
 */

var sqlite_initialized = false; // this variable is changed as 'true' when first initialize executed.

var SchemaVersionError = exports.SchemaVersionError = function(msg){
  this.name = 'SchemaVersionError';
  Error.call(this, msg);
  Error.captureStackTrace(this, arguments.callee);
};
SchemaVersionError.prototype.__proto__ = Error.prototype;

var LocalDiskStoreError = exports.LocalDiskStoreError = function(msg){
  this.name = 'LocalDiskStoreError';
  Error.call(this, msg);
  Error.captureStackTrace(this, arguments.callee);
};
LocalDiskStoreError.prototype.__proto__ = Error.prototype;

var LocalDiskStore = exports.LocalDiskStore = function(datadir, logger) {
  this.datadir = fs.realpathSync(datadir);
  this.logger = logger;

  if (! this.datadir )
    throw new LocalDiskStoreError("Invalid datadir path: " + datadir);

  var self = this;

  var stat = fs.statSync(this.datadir);
  if (! stat) {
    fs.mkdirSync(this.datadir);
  } else if (! stat.isDirectory()) {
    throw new LocalDiskStoreError("Specified path is not directory: " + datadir);
  }

  var db;
  var on_connect = function(error){
    if (error) {
      self.logger.error("failed to open sqlite3", error);
      throw new LocalDiskStoreError("sqlite3 database open error:" + error.message);
    }
    if (!db) {
      self.logger.error("failed to initialize database");
      throw new LocalDiskStoreError("db variable is unset, maybe not initialized"); }
  };
  if (!sqlite_initialized) {
    on_connect = function(error){
      if (error) {
        self.logger.error("failed to open sqlite3", error);
        throw new LocalDiskStoreError("sqlite3 database open error:" + error.message);
      }
      if (!db) {
        self.logger.error("failed to initialize database");
        throw new LocalDiskStoreError("db variable is unset, maybe not initialized");
      }
      var schemaChecker = function(cb){
        db.all("SELECT name FROM sqlite_master WHERE type='table'", function(err, rows){
          if (err) { cb(err.message); return; }
          if (rows.some(function(row){ return KNOWN_TABLES.indexOf(row.name) < 0; })) {
            self.logger.error("Database schema is of v0.");
            self.logger.error('EXECUTE: "npm run-script migrate"');
            throw new SchemaVersionError();
          }
          cb(null);
        });
      };
      var setup = [schemaChecker].concat(SQLITE_TABLE_DEFINITIONS.map(function(sql){
	    return function(callback) {
          db.run(sql, function(error){
            if (error) { callback(error.message); return; }
            callback(null);
          });
	    };
      }));
      async.series(setup, function(err,results){
	    if (err) {
          self.logger.error("failed to initialize database", err);
          throw new LocalDiskStoreError("database initialize failed");
        }
        sqlite_initialized = true;
      });
    };
  }

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

// 'CREATE TABLE IF NOT EXISTS queries (autoid INTEGER PRIMARY KEY AUTOINCREMENT, id VARCHAR(32) NOT NULL UNIQUE, datetime TEXT NOT NULL, engine TEXT DEFAULT NULL, dbname DEFAULT NULL, expression TEXT NOT NULL, state VARCHAR(32) NOT NULL, resultid VARCHAR(32) DEFAULT NULL, result DEFAULT NULL)',
LocalDiskStore.prototype.query = function(queryid, callback){
  var sql = 'SELECT id,datetime,engine,dbname,expression,state,resultid,result FROM queries WHERE id=?';
  this.db.get(sql, [queryid], function(err,row){
    if (err) { callback(err); return; }
    if (!row) { callback(null, null); return; }
    callback(null, new Query(row));
  });
};

LocalDiskStore.prototype.queries = function(queryids, callback){
  var placeholders = '(' + generatePlaceholders(queryids.length) + ')';
  var sql = 'SELECT id,datetime,engine,dbname,expression,state,resultid,result FROM queries WHERE id IN ' + placeholders;
  this.db.all(sql, queryids, function(err,rows){
    if (err) { callback(err); return; }
    callback(null, rows.map(function(r){ return new Query(r);}));
  });
};

LocalDiskStore.prototype.insertQuery = function(query, callback){
  var sql = 'INSERT INTO queries (id,datetime,engine,dbname,expression,state,resultid,result) VALUES (?,?,?,?,?,?,?,?)';
  this.db.run(sql, query.serialized(), function(err){
    if (callback) callback(err);
  });
};

LocalDiskStore.prototype.updateQuery = function(query, callback){
  this.db.run('UPDATE queries SET state=?, result=? WHERE id=?', query.serializedForUpdate(), function(err){
    if (callback) callback(err);
  });
};

LocalDiskStore.prototype.deleteQuery = function(queryid, callback){
  this.db.run('DELETE FROM queries WHERE id=?', [queryid], function(err){
    if (callback) callback(err);
  });
};

LocalDiskStore.prototype.recentQueries = function(num, callback){
  var sql = 'SELECT id,datetime,engine,dbname,expression,state,resultid,result FROM queries ORDER BY id DESC LIMIT ' + parseInt(num);
  var db = this.db;
  this.db.all(sql, function(err, rows){
    if (err) { callback(err); return; }
    callback(null, rows.map(function(r){ return new Query(r); }));
  });
};

// 'CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY AUTOINCREMENT, queryid VARCHAR(32) NOT NULL, tag VARCHAR(16) NOT NULL)'
LocalDiskStore.prototype.tagList = function(callback){
  this.db.all('SELECT DISTINCT tag FROM tags ORDER BY tag', function(err, rows){
    if (err) { callback(err); return; }
    callback(null, rows.map(function(obj){ return obj.tag; }));
  });
};

LocalDiskStore.prototype.taggedQueries = function(tag, callback){
  this.db.all('SELECT queryid FROM tags WHERE tag=?', [tag], function(err, rows){
    if (err) { callback(err); return; }
    callback(null, rows.map(function(obj){ return obj.queryid; }));
  });
};

LocalDiskStore.prototype.tags = function(queryid, callback){
  this.db.all('SELECT tag FROM tags WHERE queryid=?', [queryid], function(err, rows){
    if (err) { callback(err); return; }
    callback(null, rows.map(function(obj){ return obj.tag; }));
  });
};

LocalDiskStore.prototype.addTag = function(queryid, tag, callback){
  var self = this;
  this.db.all('SELECT queryid, tag FROM tags WHERE queryid=? AND tag=?', [queryid, tag], function(err, rows){
    if (err) { callback(err); return; }
    if (rows && rows.length > 0) {
      // specified tag already exists: ignore
      if (callback) callback(null);
      return;
    }
    self.db.run('INSERT INTO tags (queryid,tag) VALUES (?,?)', [queryid, tag], function(err){
      if (callback) callback(err);
    });
  });
};

LocalDiskStore.prototype.deleteTag = function(queryid, tag, callback){
  this.db.run('DELETE FROM tags WHERE queryid=? AND tag=?', [queryid, tag], function(err){
    if (callback) callback(err);
  });
};

LocalDiskStore.prototype.deleteTagForQuery = function(queryid, callback){
  this.db.run('DELETE FROM tags WHERE queryid=?', [queryid], function(err){
    if (callback) callback(err);
  });
};

/* result data path utilities */
LocalDiskStore.prototype.generatePathElements = function(key){
  return [this.datadir, 'results', key[0], key[1], key];
};

LocalDiskStore.prototype.generatePathFromElements = function(elements){
  return elements.join("/");
};

LocalDiskStore.prototype.generatePath = function(key){
  return this.generatePathFromElements(this.generatePathElements(key));
};

LocalDiskStore.prototype.keyExists = function(key){
  return fs.existsSync(this.generatePath(key));
};

LocalDiskStore.prototype.prepare = function(key, callback){
  var elements = this.generatePathElements(key);
  elements.pop(); // purge file basename (is not target for mkdir)

  var store = this;
  var dig = function(elements, depth, cb){
    var path = store.generatePathFromElements(elements.slice(0,depth));
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
    this.logger.warn("specified result data to write is already exists", {key: key});
    callback({message:"specified result data to write is already exists, key:" + key});
    return;
  }

  var path = this.generatePath(key);
  this.prepare(key, function(){
    fs.writeFile(path, data, 'utf8', function(err){
      callback(err);
    });
  });
};

LocalDiskStore.prototype.appendResultData = function(key, data, callback){
  var path = this.generatePath(key);
  this.prepare(key, function(){
    fs.appendFile(path, data, 'utf8', function(err){
      callback(err);
    });
  });
};

LocalDiskStore.prototype.deleteResultData = function(key, callback){
  var path = this.generatePath(key);
  fs.exists(path, function(exists){
    if (exists)
      fs.unlink(path, callback);
    else
      callback(null);
  });
};