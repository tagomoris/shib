var crypto = require('crypto');

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
  if (args.json) {
    args = JSON.parse(args.json);
    this.querystring = args.querystring;
  }
  else {
    Query.checkQueryString(args.querystring);

    this.querystring = args.querystring;
  }

  this.results = args.results || [];
  this.queryid = args.queryid || Query.generateQueryId(this.querystring, args.seed);
};

Query.prototype.serialized = function(){
  return JSON.stringify(this);
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
  q = q.replace('__TODAY__', today).replace('__YESTERDAY__', yesterday).replace('__MONTH__', month).replace('__LASTMONTH__', lastmonth);

  var re = /__(\d)DAYS_AGO__/;
  var match;
  while ((match = re.exec(q)) !== null) {
    q = q.replace('__' + match[1] + 'DAYS_AGO__', dateformat('%Y%m%d', new Date(d - 86400000 * parseInt(match[1]))));
  }

  return '--- ' + this.queryid.substr(0,8) + "\n" + q;
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

Query.generateQueryId = function(querystring, seed){
  if (!seed)
    seed = '';
  var md5sum = crypto.createHash('md5');
  md5sum.update(new Buffer(querystring + ';' + seed, 'utf8'));
  return md5sum.digest('hex');
};
