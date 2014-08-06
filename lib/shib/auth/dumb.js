var Auth = exports.Auth = function(args){
};

Auth.prototype.check = function(username, password, callback) {
  callback(null, true);
};
