var crypto = require('crypto');

var STATE_RUNNING = "running",
    STATE_DONE = "done",
    STATE_ERROR = "error";

var removeNewLines = exports.removeNewLines = function(str){
  return str.split(/\r\n|\r|\n/).join(' ');
};
var removeNewLinesAndComments = exports.removeNewLinesAndComments = function(str){
  return str.replace(/--.*$/mg, '').split(/\r\n|\r|\n/).join(' ');
};

var InvalidQueryError = exports.InvalidQueryError = function(msg){
  this.name = 'InvalidQueryError';
  this.message = msg;
  Error.captureStackTrace(this, arguments.callee);
};
InvalidQueryError.prototype = Error.prototype;

var Query = exports.Query = function(args){
  if (args.querystring)
    Query.checkQueryString(args.querystring);

  // 'SELECT id,datetime,engine,dbname,expression,state,resultid,result FROM queries';

  this.querystring = args.expression || args.querystring;

  this.queryid = args.id || args.queryid || Query.generateQueryId(this.querystring, args.seed);

  this.datetime = new Date();
  if (args.datetime)
    this.datetime = new Date(args.datetime);

  this.engine = args.engine;
  this.dbname = args.dbname;

  this.state = args.state || STATE_RUNNING;

  this.resultid = args.resultid || Query.generateResultId(this.queryid, this.datetime.toLocaleString());
  //TODO: remove all "results" from client.js
  if (args.result)
    this.result = Query.generateResult({json: args.result});
  else
    this.result = Query.generateResult(null);
};

Query.generateResult = function(args){
  if (args.json) {
    // Date object is weak to be stringified, and cannot be restored from JSON.parse()
    // loaded result object does not have executed_time/completed_time
    args = JSON.parse(args.json);
    delete args.executed_time;
    delete args.completed_time;
  }

  var r = {};

  r.error = args.error || "";
  r.lines = args.lines || null;
  r.bytes = args.bytes || null;

  r.completed_at = args.completed_at || null;
  r.completed_msec = args.completed_msec || null;
  if (! r.completed_msec && r.completed_at)
    r.completed_msec = new Date(r.completed_at).getTime();

  /* [ { name: 'x', type: 'string', comment: null },
       { name: 'cnt', type: 'bigint', comment: null } ] */
  r.schema = args.schema || [];

  return r;
};

Query.prototype.running = function(){
  return this.state === STATE_RUNNING;
};

Query.prototype.withError = function(){
  if (this.state === STATE_DONE || this.state === STATE_ERROR)
    return this.state === STATE_ERROR;
  throw new Error("called without done/error status");
};

Query.prototype.markAsExecuted = function(err, lines, bytes){
  if (err) {
    this.state = STATE_ERROR;
    this.result.error = err.message;
  }
  else {
    this.state = STATE_DONE;
  }

  if (lines)
    this.result.lines = lines;
  if (bytes)
    this.result.bytes = bytes;

  this.result.completed_time = new Date();
  this.result.completed_msec = this.result.completed_time.getTime();
  this.result.completed_at = this.result.completed_time.toLocaleString();
};

Query.prototype.addSchema = function(schemaData){
  this.result.schema = schemaData;
};

Query.prototype.serializedResult = function(){
  // Date object is weak to be stringified, and cannot be restored from JSON.parse()
  // loaded result object does not have executed_time/completed_time
  var result = this.result;
  delete result.executed_time;
  delete result.completed_time;
  return JSON.stringify(result);
};

Query.prototype.serialized = function(){
  // id,datetime,engine,dbname,expression,state,resultid,result
  return [
    this.queryid,
    this.datetime.toJSON(),
    this.engine,
    this.dbname,
    this.querystring,
    this.state,
    this.resultid,
    this.serializedResult()
  ];
};

Query.prototype.serializedForUpdate = function(){
  // UPDATE queries SET state=?, result=? WHERE id=?
  return [
    this.state,
    this.serializedResult(),
    this.queryid
  ];
};

Query.prototype.composed = function(){
  var q = this.querystring;

  var dateformat = function(format, date) {
    function pad2(n){return n < 10 ? '0' + n : n ;}
    switch(format) {
    case '%Y%m%d':
      return date.getFullYear() + pad2(date.getMonth() + 1) + pad2(date.getDate());
    case '%Y%m':
      return date.getFullYear() + pad2(date.getMonth() + 1);
    }
    return undefined;
  };
  var d = new Date();

  var today = dateformat('%Y%m%d', d);
  var month = dateformat('%Y%m', d);
  var yesterday = dateformat('%Y%m%d', new Date(d - 86400000));
  var lastmonth = dateformat('%Y%m', new Date(d - 86400000 * (d.getDate() + 1)));
  q = q.replace(/__TODAY__/g, today)
       .replace(/__YESTERDAY__/g, yesterday)
       .replace(/__MONTH__/g, month)
       .replace(/__LASTMONTH__/g, lastmonth);

  var re = /__(\d)DAYS_AGO__/;
  var match;
  while ((match = re.exec(q)) !== null) {
    var regexp = RegExp('__' + match[1] + 'DAYS_AGO__', 'g');
    q = q.replace(regexp, dateformat('%Y%m%d', new Date(d - 86400000 * parseInt(match[1]))));
  }
  return q;
};

Query.checkQueryString = function(querystring){
  if (! querystring || querystring.length < 1) {
    throw new InvalidQueryError("Blank or too short query.");
  }
  if (querystring.indexOf(';') >= 0) {
    throw new InvalidQueryError("Two or more queries detected! Semicolon prohibited.");
  }
  // query type of create/drop/..../alter/load/insert/explain too many query types!!!
  // white list now...
  if (/^\s*select .* from .+$/i.exec(removeNewLinesAndComments(querystring))){
    return;
  }
  throw new InvalidQueryError("Invalid query type. Allowed methods: 'SELECT'.");
};

Query.parseTableNames = function(querystring){
  var result = [];

  var regexp = /\s(?:FROM|JOIN)\s+([_.a-zA-Z0-9]+)/img; // ignoreCase, multiline, global
  var match;
  while ( (match = regexp.exec(querystring)) ) {
    var specified = match[1];
    if (specified.indexOf('.') != -1) {
      result.push( specified.split(".", 2).reverse() );
    } else {
      result.push( [ specified, null] );
    }
  }

  return result;
};

Query.generateQueryId = function(querystring, seed){
  if (!seed)
    seed = '';
  var md5sum = crypto.createHash('md5');
  md5sum.update(new Buffer(querystring + ';' + seed, 'utf8'));
  return md5sum.digest('hex');
};

Query.generateResultId = function(queryid, executed_at) {
  var md5sum = crypto.createHash('md5');
  md5sum.update(new Buffer(queryid + executed_at, 'utf8'));
  return md5sum.digest('hex');
};

