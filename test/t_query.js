var testCase = require('nodeunit').testCase;
var query = require('shib/query');

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
  removeNewLines: function(test) {
    test.equals('hoge', query.removeNewLines('hoge'));
    test.equals("hoge", query.removeNewLines("hoge\n"));
    test.equals("hoge", query.removeNewLines("ho\nge"));
    test.equals("hoge", query.removeNewLines("\nhoge"));
    test.equals("hoge", query.removeNewLines("\nhoge\n"));
    test.equals("hoge", query.removeNewLines("\nho\n\nge\n"));
    test.done();
  },
  checkQueryString: function(test) {
    test.done();
  },
  generateQueryId: function(test) {
    test.done();
  },
  tearDown: function (callback) {
    // clean up
    callback();
  }
});