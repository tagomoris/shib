var crypto = require('crypto');

var ResultMeta = exports.ResultMeta = function(args){
  if (args.json) {
    args = JSON.parse(args.json);
  }
  else {
    if (! args.queryid || ! args.executed_at)
      throw new TypeError("both of queryid and executed_at needed!");
  }
  this.queryid = args.queryid || null;
  this.executed_at = args.executed_at || null;
  this.resultid = args.resultid || ResultMeta.generateResultId(this.queryid, this.executed_at);
};

ResultMeta.prototype.serialized = function(){
  return JSON.stringify(this);
};

ResultMeta.generateResultId = function(queryid, executed_at) {
  var md5sum = crypto.createHash('md5');
  md5sum.update(new Buffer(queryid + executed_at, 'utf8'));
  return md5sum.digest('hex');
};
