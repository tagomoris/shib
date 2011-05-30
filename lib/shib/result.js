var crypto = require('crypto');

var RESULT_STATE_RUNNING = exports.RESULT_STATE_RUNNING = "running",
    RESULT_STATE_DONE = exports.RESULT_STATE_DONE = "done",
    RESULT_STATE_ERROR = exports.RESULT_STATE_ERROR = "error";

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

  /* [ { name: 'x', type: 'string', comment: null },
       { name: 'cnt', type: 'bigint', comment: null } ] */
  this.schema = args.schema || [];
};

Result.prototype.markAsExecuted = function(err){
  if (err) {
    this.state = RESULT_STATE_ERROR;
    this.error = err.message;
  }
  else {
    this.state = RESULT_STATE_DONE;
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
