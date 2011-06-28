var testCase = require('nodeunit').testCase;
var builder = require('shib/simple_csv_builder').SimpleCSVBuilder;

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
  escape: function (test) {
    test.equals(builder.escape(''), '');
    test.equals(builder.escape('hoge'), 'hoge');
    test.equals(builder.escape('hoge hoge'), 'hoge hoge');
    test.equals(builder.escape('hoge,pos'), 'hoge,pos');
    test.equals(builder.escape('hoge"pos'), 'hoge""pos');
    test.equals(builder.escape('hoge\'pos'), 'hoge\'pos');
    test.equals(builder.escape('"hoge"pos'), '""hoge""pos');
    test.equals(builder.escape('hoge""pos'), 'hoge""""pos');
    test.done();
  },
  build: function (test) {
    test.equals(builder.build([]), '');
    test.equals(builder.build(['']), '""\n');
    test.equals(builder.build(['','']), '"",""\n');
    test.equals(builder.build(['a']), '"a"\n');
    test.equals(builder.build(['aa']), '"aa"\n');
    test.equals(builder.build(['a"b']), '"a""b"\n');
    test.equals(builder.build(['a','b']), '"a","b"\n');
    test.equals(builder.build(['a"x','b']), '"a""x","b"\n');
    test.equals(builder.build(['a"','b']), '"a""","b"\n');
    test.equals(builder.build(['a','b', 'c']), '"a","b","c"\n');
    test.equals(builder.build(['a','b', '', 'c']), '"a","b","","c"\n');
    test.equals(builder.build(['a','b', null, 'c']), '"a","b","","c"\n');
    test.equals(builder.build(['a','b', undefined, 'c']), '"a","b","","c"\n');
    test.equals(builder.build(['a','b', 0, 'c']), '"a","b","0","c"\n');
    test.done();
  }
});
