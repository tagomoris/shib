var crypto = require('crypto');

var RESULT_STATE_RUNNING = "running",
    RESULT_STATE_DONE = "done",
    RESULT_STATE_ERROR = "error";

var Result = exports.Result = function(args){
  if (args.json) {
    args = JSON.parse(args.json);
  }
  else {
    if (! args.queryid || ! args.executed_time)
      throw new TypeError("both of queryid and executed_at needed!");
  }
  this.queryid = args.queryid || null;
  this.executed_time = args.executed_time || null;
  this.executed_msec = this.executed_time ? this.executed_time.getTime() : null;
  this.executed_at = this.executed_time ? this.executed_time.toLocaleString() : null;
  this.resultid = args.resultid || Result.generateResultId(this.queryid, this.executed_at);
  this.state = args.state || RESULT_STATE_RUNNING;
  this.error = args.error || "";
  this.lines = args.lines || null;
  this.bytes = args.bytes || null;
  this.completed_time = args.completed_time || null;
  this.completed_msec = this.completed_time ? this.completed_time.getTime() : null;
  this.completed_at = this.completed_time ? this.completed_time.toLocaleString() : null;

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
  }
  else {
    this.state = RESULT_STATE_DONE;
  }
  this.completed_time = new Date();
  this.completed_msec = this.completed_time.getTime();
  this.completed_at = this.completed_time.toLocaleString();
};

Result.prototype.serialized = function(){
  return JSON.stringify(this);
};

Result.generateResultId = function(queryid, executed_at) {
  var md5sum = crypto.createHash('md5');
  md5sum.update(new Buffer(queryid + executed_at, 'utf8'));
  return md5sum.digest('hex');
};
