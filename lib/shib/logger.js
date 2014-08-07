var util = require('util');

var TRACE = 0
  , DEBUG = 1
  , INFO = 2
  , WARN = 3
  , ERROR = 4
  , FATAL = 5;

var LOGLEVELS = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'];

var LOG_FORMAT_SIMPLE    = "%s [%s] : %s"
  , LOG_FORMAT_WITH_DUMP = "%s [%s] : %s, %j";

var Logger = exports.Logger = function(args){
  this.level = args.level || INFO;
};

Logger.prototype.log = function(level, msg, optional){
  if (level < this.level)
    return;

  var date = new Date();
  var log_string;

  if (optional)
    log_string = util.format(LOG_FORMAT_WITH_DUMP, date.toLocaleString(), LOGLEVELS[level], msg, optional);
  else
    log_string = util.format(LOG_FORMAT_SIMPLE, date.toLocaleString(), LOGLEVELS[level], msg);

  util.puts(log_string);
};

Logger.prototype.trace = function(msg, optional){
  this.log(TRACE, msg, optional);
};

Logger.prototype.debug = function(msg, optional){
  this.log(DEBUG, msg, optional);
};

Logger.prototype.info = function(msg, optional){
  this.log(INFO, msg, optional);
};

Logger.prototype.warn = function(msg, optional){
  this.log(WARN, msg, optional);
};

Logger.prototype.error = function(msg, optional){
  this.log(ERROR, msg, optional);
};

Logger.prototype.fatal = function(msg, optional){
  this.log(FATAL, msg, optional);
};
