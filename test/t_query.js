var testCase = require('nodeunit').testCase;
var query = require('shib/query'),
    Query = query.Query;


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
    test.equals(query.removeNewLines('hoge'), 'hoge');
    test.equals(query.removeNewLines("hoge\n"), 'hoge ');
    test.equals(query.removeNewLines("ho\nge"), 'ho ge');
    test.equals(query.removeNewLines("\nhoge"), ' hoge');
    test.equals(query.removeNewLines("\nhoge\n"), ' hoge ');
    test.equals(query.removeNewLines("\nho\n\nge\n"), ' ho  ge ');
    test.done();
  },
  checkKeywordString: function(test) {
    test.throws(function(){Query.checkKeywordString(' ');});
    test.throws(function(){Query.checkKeywordString('\\n');});
    test.throws(function(){Query.checkKeywordString('\\t');});
    test.throws(function(){Query.checkKeywordString("'");});
    test.throws(function(){Query.checkKeywordString('"');});
    test.throws(function(){Query.checkKeywordString(';');});
    test.throws(function(){Query.checkKeywordString('x,1');});
    test.throws(function(){Query.checkKeywordString('x;1');});
    test.doesNotThrow(function(){Query.checkKeywordString('000');});
    test.doesNotThrow(function(){Query.checkKeywordString('Hoge');});
    test.doesNotThrow(function(){Query.checkKeywordString('hoge');});
    test.doesNotThrow(function(){Query.checkKeywordString('hoge01');});
    test.done();
  },
  checkQueryString: function(test) {
    test.throws(function(){Query.checkQueryString('');});
    test.throws(function(){Query.checkQueryString('select count(*)');});
    test.throws(function(){Query.checkQueryString('show tables');});
    test.throws(function(){Query.checkQueryString('describe hoge_table');});
    test.throws(function(){Query.checkQueryString('create table hoge (col1 as string, col2 as bigint)');});
    test.throws(function(){Query.checkQueryString('drop table hoge');});
    test.throws(function(){Query.checkQueryString('alter table hoge (col1 as smallint)');});
    test.throws(function(){Query.checkQueryString("insert overwrite into hoge\nselect col1,col2 from hoge where moge='pos'");});
    test.doesNotThrow(function(){Query.checkQueryString("select field1,field2,count(*) as cnt from hoge_table where yyyymmdd=today()");});
    test.doesNotThrow(function(){Query.checkQueryString(" select field1,field2,count(*) as cnt from hoge_table where yyyymmdd=today()");});
    test.throws(function(){
      Query.checkQueryString("select field1,field2,count(*) as cnt from hoge_table where yyyymmdd=today(); drop table hoge_table");
    });
    test.done();
  },
  generateQueryId: function(test) {
    test.equals(Query.generateQueryId('select * from hoge where id=111', []), 'cec9ab9d980c1b3ed582471dc79eb65b');
    test.equals(Query.generateQueryId('select * from __KEY__ where id=111', ['keyword1']), '1007af7e430e26fc0a825b25295387f3');
    test.equals(Query.generateQueryId('select x,__KEY2__ from __KEY1__ where id=111', ['keyword1', 'keyword2']), 'c8b589da212d049df19e4d279eaf2c03');
    test.done();
  },
  instanciate: function(test) {
    var q1 = new Query({querystring:'select f1,f2 from hoge_table', keywords:[]});
    test.equals(q1.queryid, Query.generateQueryId('select f1,f2 from hoge_table', []));
    test.equals(q1.querystring, 'select f1,f2 from hoge_table');
    test.deepEqual(q1.keywords, []);
    test.deepEqual(q1.results, []);

    var q2 = new Query({querystring:'select f1,f2 from hoge_table where service="__KEY1__"', keywords:['news']});
    test.equals(q2.queryid, Query.generateQueryId('select f1,f2 from hoge_table where service="__KEY1__"', ['news']));
    test.equals(q2.querystring, 'select f1,f2 from hoge_table where service="__KEY1__"');
    test.deepEqual(q2.keywords, ['news']);
    test.deepEqual(q2.results, []);

    test.done();
  },
  serialized: function(test) {
    var q1 = new Query({querystring:'select f1,f2 from hoge_table', keywords:[]});
    test.deepEqual(q1.serialized(),
                   '{"querystring":"select f1,f2 from hoge_table","keywords":[],"results":[],"queryid":"c148cdb90a70ef34dab88d8e1af967a6"}');
    var q1x = new Query({json:q1.serialized()});
    test.equals(q1x.queryid, q1.queryid);
    test.equals(q1x.querystring, q1.querystring);
    test.deepEqual(q1x.keywords, q1.keywords);
    test.deepEqual(q1x.results, q1.results);

    var q2 = new Query({querystring:'select f1,f2 from hoge_table where service="__KEY__"', keywords:['news']});
    test.deepEqual(q2.serialized(),
                   '{"querystring":"select f1,f2 from hoge_table where service=\\"__KEY__\\"","keywords":["news"],"results":[],"queryid":"0e2052b37d22dd9d314e44384e81e640"}');
    var q2x = new Query({json:q2.serialized()});
    test.equals(q2x.queryid, q2.queryid);
    test.equals(q2x.querystring, q2.querystring);
    test.deepEqual(q2x.keywords, q2.keywords);
    test.deepEqual(q2x.results, q2.results);
    
    test.done();
  },
  checkPlaceholders: function(test) {
    test.doesNotThrow(function(){Query.checkPlaceholders('select * from hoge', []);});
    test.throws(function(){Query.checkPlaceholders('select * from hoge where f1=__KEY0__', ['1']);});
    test.throws(function(){Query.checkPlaceholders('select __KEY1__,__KEY2__,__KEY3__,__KEY4__,__KEY5__,__KEY6__,__KEY7__,__KEY8__,__KEY9__,__KEY10__ from hoge',
                                                   ['1','2','3','4','5','6','7','8','9','10']);});

    test.doesNotThrow(function(){Query.checkPlaceholders('select f1,f2 from __KEY__', ['1']);});
    test.throws(function(){Query.checkPlaceholders('select * from hoge', ['1']);});
    test.throws(function(){Query.checkPlaceholders('select f1,f2 from __KEY__');});
    test.throws(function(){Query.checkPlaceholders('select f1,f2 from __KEY1__');});

    test.doesNotThrow(function(){Query.checkPlaceholders('select * from __KEY2__ where f1="__KEY1__"', ['1', '2']);});
    test.throws(function(){Query.checkPlaceholders('select * from __KEY2__ where f1="__KEY__"', ['1', '2']);});
    test.throws(function(){Query.checkPlaceholders('select * from hoge', ['1', '2']);});
    test.throws(function(){Query.checkPlaceholders('select * from hoge where f1=__KEY1__', ['1', '2']);});
    test.throws(function(){Query.checkPlaceholders('select * from hoge where f1=__KEY2__', ['1', '2']);});
    test.throws(function(){Query.checkPlaceholders('select f1,f2 from __KEY1__ where f1=__KEY2__', ['1']);});
    test.throws(function(){Query.checkPlaceholders('select f1,f2 from __KEY1__ where f1=__KEY3__' ['1','2']);});

    test.done();
  },
  composed: function(test) {
    var s0 = "select f1,f2,f3,f4,f5,f6,f7,f8,f9 from xtable where f1='value1'";
    var q0 = new Query({querystring:s0});
    test.equals(q0.composed(), s0);

    var s1 = "select f1,f2,f3,f4,f5,f6,f7,f8,f9 from __KEY__ where f1='value1'";
    var q1 = new Query({querystring:s1, keywords:['moge']});
    test.equals(q1.composed(), "select f1,f2,f3,f4,f5,f6,f7,f8,f9 from moge where f1='value1'");
    var q1x = new Query({querystring:s1, keywords:['hage00']});
    test.equals(q1x.composed(), "select f1,f2,f3,f4,f5,f6,f7,f8,f9 from hage00 where f1='value1'");

    var s2 = "select f1,f2,f3,f4,f5,f6,f7,f8,f9 from xxx where f1='__KEY__' and f2='__KEY__'";
    var q2 = new Query({querystring:s2, keywords:['test']});
    test.equals(q2.composed(), "select f1,f2,f3,f4,f5,f6,f7,f8,f9 from xxx where f1='test' and f2='test'");
    var s2x = "select f1,f2,f3,f4,f5,f6,f7,f8,f9 from xxx where f1='__KEY1__' and f2='__KEY1__'";
    var q2x = new Query({querystring:s2x, keywords:['test']});
    test.equals(q2x.composed(), "select f1,f2,f3,f4,f5,f6,f7,f8,f9 from xxx where f1='test' and f2='test'");

    var s9 = "select __KEY1__,__KEY2__,__KEY3__,__KEY4__,__KEY5__,__KEY6__,__KEY7__,__KEY8__,__KEY9__ from xxx where __KEY8__='moge' and __KEY2__=10";
    var q9 = new Query({querystring:s9, keywords:['a1','b2','c3','d4','e5','f6','g7','h8','i9']});
    test.equals(q9.composed(), "select a1,b2,c3,d4,e5,f6,g7,h8,i9 from xxx where h8='moge' and b2=10");

    test.done();
  },
  tearDown: function (callback) {
    // clean up
    callback();
  }
});
