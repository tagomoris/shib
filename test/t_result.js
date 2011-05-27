var testCase = require('nodeunit').testCase;
var result = require('shib/result'),
    ResultMeta = result.ResultMeta;

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
  generateResultId: function(test) {
    test.equals(ResultMeta.generateResultId('cec9ab9d980c1b3ed582471dc79eb65b', '20110427190250'), '26203ab72560e3974b36f16cdea07085');
    test.done();
  },
  tearDown: function (callback) {
    // clean up
    callback();
  }
});
