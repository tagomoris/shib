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
  removeNewLinesAndComments: function(test) {
    test.equals(query.removeNewLinesAndComments('hoge'), 'hoge');
    test.equals(query.removeNewLinesAndComments("hoge\n"), 'hoge ');
    test.equals(query.removeNewLinesAndComments("ho\nge"), 'ho ge');
    test.equals(query.removeNewLinesAndComments("\nhoge"), ' hoge');
    test.equals(query.removeNewLinesAndComments("\nhoge\n"), ' hoge ');
    test.equals(query.removeNewLinesAndComments("\nho\n\nge\n"), ' ho  ge ');

    test.equals(query.removeNewLinesAndComments('hoge\n--pos'), 'hoge ');
    test.equals(query.removeNewLinesAndComments('hoge\n--pos\nmoge'), 'hoge  moge');
    test.equals(query.removeNewLinesAndComments('--koge\nhoge\n--pos\nmoge'), ' hoge  moge');
    test.equals(query.removeNewLinesAndComments('hoge --koge\nhoge\n--pos\nmoge'), 'hoge  hoge  moge');
    test.equals(query.removeNewLinesAndComments(' --koge\nhoge\n--pos\nmoge'), '  hoge  moge');

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
    test.doesNotThrow(function(){Query.checkQueryString("-- TEST Query \n select field1,field2,count(*) as cnt from hoge_table where yyyymmdd=today()");});
    test.done();
  },
  generateQueryId: function(test) {
    test.equals(Query.generateQueryId('select * from hoge where id=111'), 'cec9ab9d980c1b3ed582471dc79eb65b');
    test.equals(Query.generateQueryId('select x,y from hoge where id=111'), '4f9adf64784f5cb7d0f32fc67e2f387c');

    test.equals(Query.generateQueryId('select * from hoge where id=111', '201108'), '14838f269a5aed68f61bc60615d21330');
    test.equals(Query.generateQueryId('select x,y from hoge where id=111', '201108'), '5949bf0ce322195db18f045fc70c6159');
    test.done();
  },
  instanciate: function(test) {
    var q1 = new Query({querystring:'select f1,f2 from hoge_table'});
    test.equals(q1.queryid, Query.generateQueryId('select f1,f2 from hoge_table'));
    test.equals(q1.querystring, 'select f1,f2 from hoge_table');
    test.deepEqual(q1.results, []);

    var q2 = new Query({querystring:'select f1,f2 from hoge_table where service="news"'});
    test.equals(q2.queryid, Query.generateQueryId('select f1,f2 from hoge_table where service="news"'));
    test.equals(q2.querystring, 'select f1,f2 from hoge_table where service="news"');
    test.deepEqual(q2.results, []);

    var q3 = new Query({querystring:'select f1,f2 from hoge_table where service="news"', seed:'201108'});
    test.equals(q3.queryid, Query.generateQueryId('select f1,f2 from hoge_table where service="news"', '201108'));
    test.equals(q3.querystring, 'select f1,f2 from hoge_table where service="news"');
    test.deepEqual(q3.results, []);

    test.done();
  },
  serialized: function(test) {
    var q1 = new Query({querystring:'select f1,f2 from hoge_table'});
    test.deepEqual(q1.serialized(),
                   '{"querystring":"select f1,f2 from hoge_table","results":[],"queryid":"c148cdb90a70ef34dab88d8e1af967a6"}');
    var q1x = new Query({json:q1.serialized()});
    test.equals(q1x.queryid, q1.queryid);
    test.equals(q1x.querystring, q1.querystring);
    test.deepEqual(q1x.results, q1.results);

    var q2 = new Query({querystring:'select f1,f2 from hoge_table where service="news"'});
    test.deepEqual(q2.serialized(),
                   '{"querystring":"select f1,f2 from hoge_table where service=\\"news\\"","results":[],"queryid":"2734efa50f0dff08129e3485b523f782"}');
    var q2x = new Query({json:q2.serialized()});
    test.equals(q2x.queryid, q2.queryid);
    test.equals(q2x.querystring, q2.querystring);
    test.deepEqual(q2x.results, q2.results);
    
    test.done();
  },
  composed: function(test) {
    var s0 = "select f1,f2,f3,f4,f5,f6,f7,f8,f9 from xtable where f1='value1'";
    var q0 = new Query({querystring:s0});
    test.equals(q0.composed(), s0);

    var s1 = "select f1,f2,f3,f4,f5,f6,f7,f8,f9 from moge where f1='value1'";
    var q1 = new Query({querystring:s1});
    test.equals(q1.composed(), "select f1,f2,f3,f4,f5,f6,f7,f8,f9 from moge where f1='value1'");

    var s2 = "select f1,f2,f3,f4,f5,f6,f7,f8,f9 from xxx where f1='test' and f2='test'";
    var q2 = new Query({querystring:s2});
    test.equals(q2.composed(), "select f1,f2,f3,f4,f5,f6,f7,f8,f9 from xxx where f1='test' and f2='test'");

    test.done();
  },
  tearDown: function (callback) {
    // clean up
    callback();
  }
});
