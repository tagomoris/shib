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
    test.equals(query.removeNewLines("hoge\n"), 'hoge');
    test.equals(query.removeNewLines("ho\nge"), 'hoge');
    test.equals(query.removeNewLines("\nhoge"), 'hoge');
    test.equals(query.removeNewLines("\nhoge\n"), 'hoge');
    test.equals(query.removeNewLines("\nho\n\nge\n"), 'hoge');
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
    test.ok(Query.checkQueryString("select field1,field2,count(*) as cnt from hoge_table where yyyymmdd=today()"));
    test.ok(Query.checkQueryString(" select field1,field2,count(*) as cnt from hoge_table where yyyymmdd=today()"));
    test.throws(function(){
      Query.checkQueryString("select field1,field2,count(*) as cnt from hoge_table where yyyymmdd=today(); drop table hoge_table");
    });
    test.done();
  },
  generateQueryId: function(test) {
    test.equals(Query.generateQueryId('select * from hoge where id=111', []), 'cec9ab9d980c1b3ed582471dc79eb65b');
    test.equals(Query.generateQueryId('select * from hoge where id=111', ['keyword1']), 'ab54323b5bddaad03be698464e9a60a9');
    test.equals(Query.generateQueryId('select * from hoge where id=111', ['keyword1', 'keyword2']), 'aacb8f731fd4850472c3c4963cbb085f');
    test.done();
  },
  instanciate: function(test) {
    var q1 = new Query({querystring:'select f1,f2 from hoge_table', keywords:[]});
    test.equals(q1.queryid, Query.generateQueryId('select f1,f2 from hoge_table', []));
    test.equals(q1.querystring, 'select f1,f2 from hoge_table');
    test.deepEqual(q1.keywords, []);
    test.deepEqual(q1.results, []);

    var q2 = new Query({querystring:'select f1,f2 from hoge_table where service="KEYWORD1"', keywords:['news']});
    test.equals(q2.queryid, Query.generateQueryId('select f1,f2 from hoge_table where service="KEYWORD1"', ['news']));
    test.equals(q2.querystring, 'select f1,f2 from hoge_table where service="KEYWORD1"');
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

    var q2 = new Query({querystring:'select f1,f2 from hoge_table where service="KEYWORD1"', keywords:['news']});
    test.deepEqual(q2.serialized(),
                   '{"querystring":"select f1,f2 from hoge_table where service=\\"KEYWORD1\\"","keywords":["news"],"results":[],"queryid":"60cb2730c29c713e8cac95b605c080f5"}');
    var q2x = new Query({json:q2.serialized()});
    test.equals(q2x.queryid, q2.queryid);
    test.equals(q2x.querystring, q2.querystring);
    test.deepEqual(q2x.keywords, q2.keywords);
    test.deepEqual(q2x.results, q2.results);
    
    test.done();
  },
  tearDown: function (callback) {
    // clean up
    callback();
  }
});
