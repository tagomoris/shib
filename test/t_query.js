var testCase = require('nodeunit').testCase;

var query = require('shib/query');

module.exports = testCase({
  /*
  setUp: function (callback) {
    this.foo = 'bar';
    callback();
  },
  tearDown: function (callback) {
    // clean up
    callback();
  },
   */
  test1: function (test) {
    test.equals('bar', 'bar');
    test.done();
  }
});