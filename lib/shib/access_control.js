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

AccessControl.prototype.visible = function(dbname){
  if (! this.databases[dbname])
    return (this.default_rule === "allow");

  // this.databases[dbname] exists

  var dbrule = this.databases[dbname];
  if (dbrule['default'] === "deny" && (dbrule.allow || []).length < 1)
    // specified db configured db-scope denied
    return false;

  // visible tables are exists -> db visible
  return true;
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
