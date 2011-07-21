var testCase = require('nodeunit').testCase;
var mock = require('ThriftHiveMock');

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
  generate_tablename: function(test) {
    var tname = null;
    tname = mock.generate_tablename();
    test.ok(/^[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d)?)?)?$/.exec(tname));
    tname = mock.generate_tablename();
    test.ok(/^[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d)?)?)?$/.exec(tname));
    tname = mock.generate_tablename();
    test.ok(/^[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d)?)?)?$/.exec(tname));
    tname = mock.generate_tablename();
    test.ok(/^[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d)?)?)?$/.exec(tname));
    tname = mock.generate_tablename();
    test.ok(/^[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d)?)?)?$/.exec(tname));
    tname = mock.generate_tablename();
    test.ok(/^[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d)?)?)?$/.exec(tname));
    tname = mock.generate_tablename();
    test.ok(/^[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d)?)?)?$/.exec(tname));
    tname = mock.generate_tablename();
    test.ok(/^[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d)?)?)?$/.exec(tname));
    tname = mock.generate_tablename();
    test.ok(/^[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d)?)?)?$/.exec(tname));
    tname = mock.generate_tablename();
    test.ok(/^[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d)?)?)?$/.exec(tname));
    tname = mock.generate_tablename();
    test.ok(/^[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d)?)?)?$/.exec(tname));
    tname = mock.generate_tablename();
    test.ok(/^[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d(_[a-z]{1,3}\d)?)?)?$/.exec(tname));
    test.done();
  },
  generate_subtree: function(test){
    test.deepEqual(mock.generate_subtree('xxx1'), ['xxx=0']);
    test.deepEqual(mock.generate_subtree('xxx3'), ['xxx=0','xxx=1','xxx=2']);
    test.deepEqual(mock.generate_subtree('xxx2'), ['xxx=0','xxx=1']);
    test.deepEqual(mock.generate_subtree('xxx10'), ['xxx=0','xxx=1','xxx=2','xxx=3','xxx=4','xxx=5','xxx=6','xxx=7','xxx=8','xxx=9']);

    test.deepEqual(mock.generate_subtree('xxy1_abc2'), ['xxy=0/abc=0', 'xxy=0/abc=1']);
    test.deepEqual(mock.generate_subtree('xxy1_abc3'), ['xxy=0/abc=0', 'xxy=0/abc=1', 'xxy=0/abc=2']);
    test.deepEqual(mock.generate_subtree('xxy2_abc2'), ['xxy=0/abc=0', 'xxy=0/abc=1', 'xxy=1/abc=0', 'xxy=1/abc=1']);
    test.deepEqual(mock.generate_subtree('xxy1_abc2_ppp2'), ['xxy=0/abc=0/ppp=0', 'xxy=0/abc=0/ppp=1', 'xxy=0/abc=1/ppp=0', 'xxy=0/abc=1/ppp=1']);
    test.done();
  }
});
