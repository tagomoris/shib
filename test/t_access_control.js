var testCase = require('nodeunit').testCase;
var access_control = require('shib/access_control'),
    AccessControl = access_control.AccessControl;

module.exports = testCase({
  /*
  setUp: function (callback) {
    this.foo = 'bar';
    callback();
  },
   */
  test1: function (test) {
    test.equals('bar', 'bar');
    test.done();
  },
  default_rule: function(test){
    var rule = {};
    var acl = new AccessControl(rule);

    test.ok(acl.visible('default'));
    test.ok(acl.visible('db1'));

    test.ok(acl.allowed('t1', 'default'));
    test.ok(acl.allowed('t2', 'default'));
    test.ok(acl.allowed('WhatsTable', 'db1'));

    test.done();
  },
  default_allowed: function(test){
    var rule = {
      default: "allow",
      databases: {
        secret: { default: "deny" },
        secret2: { default: "deny", allow: ["t1"] }
      }
    };
    var acl = new AccessControl(rule);

    test.ok(acl.visible('default'));
    test.ok(! acl.visible('secret'));
    test.ok(acl.visible('secret2'));

    test.ok(acl.allowed('t1', 'default'));
    test.ok(acl.allowed('t2', 'default'));
    test.ok(acl.allowed('WhatsTable', 'db1'));

    test.ok(! acl.allowed('table', 'secret'));

    test.ok(! acl.allowed('table', 'secret2'));
    test.ok(acl.allowed('t1', 'secret2'));

    test.done();
  },
  default_denied: function(test){
    var rule = {
      default: "deny",
      databases: {
        default: { default: "allow" },
        test: { default: "allow", deny: ["IDMaster", "secretTest"] },
        data: { default: "deny", allow: ["t1", "t2"] }
      }
    };
    var acl = new AccessControl(rule);

    test.ok(! acl.visible("unknown"));
    test.ok(acl.visible("default"));
    test.ok(acl.visible("test"));
    test.ok(acl.visible("data"));

    test.ok(acl.allowed('t1', 'default'));
    test.ok(acl.allowed('t2', 'default'));

    test.ok(! acl.allowed('WhatsTable', 'db1'));

    test.ok(acl.allowed('table', 'test'));
    test.ok(acl.allowed('t1', 'test'));
    test.ok(! acl.allowed('IDMaster', 'test'));
    test.ok(! acl.allowed('secretTest', 'test'));

    test.ok(! acl.allowed('table', 'data'));
    test.ok(! acl.allowed('table2', 'data'));
    test.ok(acl.allowed('t1', 'data'));
    test.ok(acl.allowed('t2', 'data'));

    test.done();
  },
  tearDown: function (callback) {
    // clean up
    callback();
  }
});
