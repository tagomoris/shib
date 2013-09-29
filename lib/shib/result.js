var crypto = require('crypto');

var RESULT_STATE_RUNNING = "running",
    RESULT_STATE_DONE = "done",
    RESULT_STATE_ERROR = "error";

var Result = exports.Result = function(args){
  if (args.json) {
    args = JSON.parse(args.json);
  }
  else {
    if (! args.queryid || ! args.executed_at)
      throw new TypeError("both of queryid and executed_at needed!");
  }
  this.queryid = args.queryid || null;
  this.executed_at = args.executed_at || null;
  this.resultid = args.resultid || Result.generateResultId(this.queryid, this.executed_at);
  this.state = args.state || RESULT_STATE_RUNNING;
  this.error = args.error || "";
  this.lines = args.lines || null;
  this.bytes = args.bytes || null;
  this.completed_at = args.completed_at || null;
  this.name = args.name || "";
  this.description = args.description || "";

  /* [ { name: 'x', type: 'string', comment: null },
       { name: 'cnt', type: 'bigint', comment: null } ] */
  this.schema = args.schema || [];
};

Result.prototype.running = function(){
  return this.state === RESULT_STATE_RUNNING;
};

Result.prototype.withError = function(){
  if (this.state === RESULT_STATE_DONE || this.state === RESULT_STATE_ERROR)
    return this.state === RESULT_STATE_ERROR;
  throw new Error("called without done/error status");
};

Result.prototype.markAsExecuted = function(err){
  if (err) {
    this.state = RESULT_STATE_ERROR;
    this.error = err.message;
    this.completed_at = (new Date()).toLocaleString();
  }
  else {
    this.state = RESULT_STATE_DONE;
    this.completed_at = (new Date()).toLocaleString();
  }
};

Result.prototype.serialized = function(){
  return JSON.stringify(this);
};

Result.generateResultId = function(queryid, executed_at) {
  var md5sum = crypto.createHash('md5');
  md5sum.update(new Buffer(queryid + executed_at, 'utf8'));
  return md5sum.digest('hex');
};
