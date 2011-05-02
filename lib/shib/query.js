var crypto = require('crypto');

// queryid: md5 of (querystring + ';' + keywordlist.join(','))
// query: Query object, stored in KT (shib.kch)
//   {queryid, querystring, [keyword1, keyword2, ...], [{executed_date1:resultid1}, {executed_date2:resultid2}, {..}, ...]}

var removeNewLines = exports.removeNewLines = function(str){
  if (str.indexOf("\n") < 0)
    return str;
  var ret = str;
  while(ret.indexOf("\n") >= 0){
    ret = ret.replace("\n", "");
  }
  return ret;
};

var InvalidQueryError = exports.InvalidQueryError = function(msg){
  this.name = 'InvalidQueryError';
  Error.call(this, msg);
  Error.captureStackTrace(this, arguments.callee);
};
InvalidQueryError.prototype.__proto__ = Error.prototype;

var Query = exports.Query = function(args){
  if (args.json) {
    args = JSON.parse(args.json);
  }
  else {
    if (! Query.checkQueryString(args.querystring)){
      // pattern of 'return false' will not appear...
      throw new InvalidQueryError("unknown result of Query.checkQueryString");
    }
  }
  this.querystring = args.querystring;

  this.keywords = args.keywords || [];
  this.results = args.results || [];

  this.queryid = args.queryid || Query.generateQueryId(this.querystring, this.keywords);
};

Query.prototype.serialized = function(){
  return JSON.stringify(this);
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
  if (/^\s*select .* from .+$/i.exec(removeNewLines(querystring))){
    return true;
  }
  throw new InvalidQueryError("Invalid query type. Allowed methods: 'SELECT'.");
};

Query.generateQueryId = function(querystring, keywords){
  var md5sum = crypto.createHash('md5');
  md5sum.update(new Buffer(querystring + ';' + keywords.join(","), 'utf8'));
  return md5sum.digest('hex');
};
