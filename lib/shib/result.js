var crypto = require('crypto');

// resultid: md5 of (queryid + YYYYmmddHHMMSS of executed date)
// result: Result object from (resultid, queryid, executed_at, resultdata)

var Result = exports.Result = function(args){
  if (args.json) {
    args = JSON.parse(args.json);
  }
  else {
    /* check or not needed */
  }
  this.resultid = args.resultid || null;
  this.queryid = args.queryid || null;
  this.executed_at = args.executed_at || null;
  this.resultdata = args.resultdata || null;
};

Result.generateResultId = function(queryid, executed_at) {
  var md5sum = crypto.createHash('md5');
  md5sum.update(new Buffer(queryid + executed_at, 'utf8'));
  return md5sum.digest('hex');
};