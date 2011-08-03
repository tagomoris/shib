var crypto = require('crypto');

var removeNewLines = exports.removeNewLines = function(str){
  return str.split('\n').join(' ');
};
var removeNewLinesAndComments = exports.removeNewLinesAndComments = function(str){
  return str.replace(/--.*$/mg, '').split('\n').join(' ');
};

var InvalidQueryError = exports.InvalidQueryError = function(msg){
  this.name = 'InvalidQueryError';
  this.message = msg;
  Error.captureStackTrace(this, arguments.callee);
};
InvalidQueryError.prototype = Error.prototype;

var Query = exports.Query = function(args){
  if (args.json) {
    args = JSON.parse(args.json);
    this.querystring = args.querystring;
    this.keywords = args.keywords || [];
  }
  else {
    Query.checkQueryString(args.querystring);

    this.querystring = args.querystring;
    this.keywords = args.keywords || [];

    this.keywords.forEach(function(k){
      Query.checkKeywordString(k);
    });
    Query.checkPlaceholders(this.querystring, this.keywords);
  }

  this.results = args.results || [];
  this.queryid = args.queryid || Query.generateQueryId(this.querystring, this.keywords);
};

Query.prototype.serialized = function(){
  return JSON.stringify(this);
};

Query.prototype.composed = function(){
  if (this.keywords.length < 1) {
    return this.querystring;
  }
  if (this.keywords.length == 1) {
    return this.querystring.replace(/__KEY1?__/g, this.keywords[0]);
  }

  var q = this.querystring;
  for (var i = 1; i <= this.keywords.length; i++) {
    var re = new RegExp('__KEY' + i + '__', 'g');
    q = q.replace(re, this.keywords[i-1]);
  }
  return q;
};

Query.checkKeywordString = function(keywordstring){
  if (/^[_.0-9a-zA-Z]+$/.exec(keywordstring))
    return;
  throw new InvalidQueryError("Invalid characters exist in keyword, only '_.0-9a-zA-Z' are allowed");
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

Query.checkPlaceholders = function(querystring, keywords){
  var q = querystring;

  if (q.match(/__KEY\d{2,}__/))
    throw new InvalidQueryError("Cannot use 10 or more sequencial placeholders such as __KEY10__");

  if (q.match(/__KEY__/)) {
    if (q.match(/__KEY\d__/))
      throw new InvalidQueryError("Cannot use both default single placeholder __KEY__ and sequencial placeholders such as __KEY1__");
    if (keywords.length < 1)
      throw new InvalidQueryError("No one keyword specified but __KEY__ placeholder used");
    if (keywords.length > 1)
      throw new InvalidQueryError("Two or more keywords specified with single placeholder __KEY__");
    return true;
  }

  if (q.match(/__KEY\d__/)) {
    if (keywords.length < 1)
      throw new InvalidQueryError();
    for (var i = 1 ; i <= keywords.length ; i++) {
      var ph = '__KEY' + i.toString() + '__';
      if (q.indexOf(ph) < 0)
        throw new InvalidQueryError("Required placeholder " + ph + "not exists");
    }
    var re = /__KEY(\d)__/g;
    var matched;
    while ((matched = re.exec(q)) != null) {
      if (matched[1] < 1 || matched[1] > keywords.length)
        throw new InvalidQueryError("Keyword not exists matches with placeholder " + matched[0]);
    }
    return true;
  }

  // without placeholders
  if (keywords.length > 0)
    throw new InvalidQueryError("No one placeholder specified for input keywords");
  return true;
};

Query.generateQueryId = function(querystring, keywords){
  var md5sum = crypto.createHash('md5');
  md5sum.update(new Buffer(querystring + ';' + keywords.join(","), 'utf8'));
  return md5sum.digest('hex');
};
