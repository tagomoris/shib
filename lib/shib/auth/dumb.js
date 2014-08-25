var Auth = exports.Auth = function(args){
};

Auth.prototype.check = function(req, callback) {
  callback(null, {username: req.body.username, password: req.body.password});
};

Auth.prototype.acl_delegators = function(required_always, username, req) {
  // allowed(tablename, dbname), visible(dbname)
  return [function(t,d){return true;}, function(d){return true;}];
};
