var AccessControl = exports.AccessControl = function(config){
  this.default_rule = config['default'] || "allow";
  this.databases = config['databases'] || [];
};

AccessControl.prototype.allowed = function(tablename, dbname){
  var dbconf = this.databases[dbname];
  if (! dbconf)
    return (this.default_rule === "allow");
  return this.checkDBPrivilege(dbconf, tablename);
};

AccessControl.prototype.checkDBPrivilege = function(dbconf, tablename){
  if (dbconf['default'] === "allow") {
    var deny = dbconf.deny || [];
    return ( deny.indexOf(tablename) === -1 );
  } else { // default deny
    var allow = dbconf.allow || [];
    return ( allow.indexOf(tablename) >= 0 );
  }
};
