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
    test.equals('hoge', query.removeNewLines('hoge'));
    test.equals("hoge", query.removeNewLines("hoge\n"));
    test.equals("hoge", query.removeNewLines("ho\nge"));
    test.equals("hoge", query.removeNewLines("\nhoge"));
    test.equals("hoge", query.removeNewLines("\nhoge\n"));
    test.equals("hoge", query.removeNewLines("\nho\n\nge\n"));
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
  tearDown: function (callback) {
    // clean up
    callback();
  }
});