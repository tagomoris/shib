var testCase = require('nodeunit').testCase;
var query = require('shib/query'),
    Query = query.Query;

var convertSerializedQueryToArgs = function(array){
  var obj = {
    id: array[0],
    datetime: array[1],
    scheduled: array[2],
    engine: array[3] || null,
    dbname: array[4] || null,
    querystring: array[5],
    state: array[6],
    resultid: array[7],
    result: array[8]
  };
  return obj;
};

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
    test.doesNotThrow(function(){Query.checkQueryString("with tbl as (select id from source where id > 10) select count(id) from tbl");});
    test.doesNotThrow(function(){Query.checkQueryString(" with tbl as (select id from source where id > 10) select count(id) from tbl");});
    test.doesNotThrow(function(){Query.checkQueryString("explain select field1,field2,count(*) as cnt from hoge_table where yyyymmdd=today()");});
    test.doesNotThrow(function(){Query.checkQueryString(" explain select field1,field2,count(*) as cnt from hoge_table where yyyymmdd=today()");});
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
    test.deepEqual(q1.result, Query.generateResult(null));

    var q2 = new Query({querystring:'select f1,f2 from hoge_table where service="news"'});
    test.equals(q2.queryid, Query.generateQueryId('select f1,f2 from hoge_table where service="news"'));
    test.equals(q2.querystring, 'select f1,f2 from hoge_table where service="news"');
    test.deepEqual(q2.result, Query.generateResult(null));

    var q3 = new Query({querystring:'select f1,f2 from hoge_table where service="news"', seed:'201108'});
    test.equals(q3.queryid, Query.generateQueryId('select f1,f2 from hoge_table where service="news"', '201108'));
    test.equals(q3.querystring, 'select f1,f2 from hoge_table where service="news"');
    test.deepEqual(q3.result, Query.generateResult(null));

    test.done();
  },
  serialized: function(test) {
    // id,datetime,engine,dbname,expression,state,resultid,result

    var q1 = new Query({querystring:'select f1,f2 from hoge_table'});
    test.deepEqual(
        q1.serialized(),
        [
          "c148cdb90a70ef34dab88d8e1af967a6",
          q1.datetime.toJSON(),
          null,
          undefined,
          undefined,
          "select f1,f2 from hoge_table",
          'running',
          Query.generateResultId("c148cdb90a70ef34dab88d8e1af967a6", q1.datetime.toLocaleString()),
          '{"error":"","lines":null,"bytes":null,"completed_at":null,"completed_msec":null,"schema":[]}'
        ]
    );
    var q1x = new Query(convertSerializedQueryToArgs(q1.serialized()));
    test.equals(q1x.queryid, q1.queryid);
    test.equals(q1x.querystring, q1.querystring);
    test.equals(q1x.state, q1.state);
    test.equals(q1x.resultid, q1.resultid); // resultid depends on executed_at (default, current time)
    test.deepEqual(q1x.result, q1.result);

    var q2 = new Query({querystring:'select f1,f2 from hoge_table where service="news"'});
    test.deepEqual(
        q2.serialized(),
        [
          "2734efa50f0dff08129e3485b523f782",
          q2.datetime.toJSON(),
          null,
          undefined,
          undefined,
          'select f1,f2 from hoge_table where service="news"',
          'running',
          Query.generateResultId("2734efa50f0dff08129e3485b523f782", q2.datetime.toLocaleString()),
          '{"error":"","lines":null,"bytes":null,"completed_at":null,"completed_msec":null,"schema":[]}'
        ]
    );
    var q2x = new Query(convertSerializedQueryToArgs(q2.serialized()));
    test.equals(q2x.queryid, q2.queryid);
    test.equals(q2x.querystring, q2.querystring);
    test.equals(q2x.state, q2.state);
    test.equals(q2x.resultid, q2.resultid);
    test.deepEqual(q2x.result, q2.result);
    
    test.done();
  },
  serializedForUpdate: function(test){
    var data = (new Query({querystring:'select f1,f2 from hoge_table'})).serializedForUpdate();
    test.deepEqual(
        data,
        [
          'running',
          '{"error":"","lines":null,"bytes":null,"completed_at":null,"completed_msec":null,"schema":[]}',
          "c148cdb90a70ef34dab88d8e1af967a6"
        ]
    );
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
  parseTableNames: function(test) {
    var q0 = 'SELECT * FROM t1';
    test.deepEqual(Query.parseTableNames(q0), [['t1', null]]);

    var q1 = 'SELECT * FROM db1.sales WHERE amount > 10 AND region = "US"';
    test.deepEqual(Query.parseTableNames(q1), [['sales', 'db1']]);

    var q2 = 'SELECT page_views.* FROM page_views\nWHERE page_views.date >= "2008-03-01" \nAND page_views.date <= "2008-03-31"';
    test.deepEqual(Query.parseTableNames(q2), [['page_views', null]]);

    var q3 = 'SELECT * FROM Source TABLESAMPLE(BUCKET 3 OUT OF 32 ON rand()) s;';
    test.deepEqual(Query.parseTableNames(q3), [['Source', null]]);

    var q4 = "SELECT u.id, actions.date\nFROM (\n    SELECT av.uid AS uid\n    FROM action_video av\n    WHERE av.date = '2008-06-03'\n    UNION ALL\n    SELECT ac.uid AS uid\n    FROM action_comment ac\n    WHERE ac.date = '2008-06-03'\n ) actions JOIN users u ON (u.id = actions.uid)";
    test.deepEqual(Query.parseTableNames(q4), [['action_video', null], ['action_comment', null], ['users', null]]);

    var j0 = 'SELECT a.* FROM a JOIN b ON (a.id = b.id AND a.department = b.department)';
    test.deepEqual(Query.parseTableNames(j0), [['a', null], ['b', null]]);

    var j1 = 'SELECT a.val, b.val, c.val FROM db1.a JOIN db2.b ON (a.key = b.key1) JOIN c ON (c.key = b.key2)';
    test.deepEqual(Query.parseTableNames(j1), [['a', 'db1'], ['b', 'db2'], ['c', null]]);

    var j2 = 'SELECT a.val, b.val FROM a LEFT OUTER JOIN b ON (a.key=b.key)';
    test.deepEqual(Query.parseTableNames(j2), [['a', null], ['b', null]]);

    var j3 = 'SELECT a.key, a.val\nFROM a LEFT SEMI JOIN b on (a.key = b.key)';
    test.deepEqual(Query.parseTableNames(j3), [['a', null], ['b', null]]);

    var j4 = 'SELECT page_views.*\nFROM page_views JOIN dim_users\n  ON (page_views.user_id = dim_users.id AND page_views.date >= "2008-03-01" AND page_views.date <= "2008-03-31")';
    test.deepEqual(Query.parseTableNames(j4), [['page_views', null], ['dim_users', null]]);

    /*
select idOne, idTwo, value FROM
  ( select idOne, idTwo, value FROM
    bigTable JOIN smallTableOne on (bigTable.idOne = smallTableOne.idOne)                                                  
  ) firstjoin                                                            
  JOIN                                                                 
    smallTableTwo on (firstjoin.idTwo = smallTableTwo.idTwo)
     */
    var j5 = 'select idOne, idTwo, value FROM\n  ( select idOne, idTwo, value FROM\n    bigTable JOIN smallTableOne on (bigTable.idOne = smallTableOne.idOne)  \n  ) firstjoin  \n  JOIN   \n  smallTableTwo on (firstjoin.idTwo = smallTableTwo.idTwo) ';
    test.deepEqual(Query.parseTableNames(j5), [['bigTable', null], ['smallTableOne', null], ['smallTableTwo', null]]);

    var s0 = 'SELECT col\nFROM (\n  SELECT a+b AS col\n  FROM t1\n) t2';
    test.deepEqual(Query.parseTableNames(s0), [['t1', null]]);

    var s1 = 'SELECT t3.col FROM ( SELECT a+b AS col FROM d1.t1 UNION ALL SELECT c+d AS col FROM d2.t2 ) t3';
    test.deepEqual(Query.parseTableNames(s1), [['t1', 'd1'], ['t2', 'd2']]);

    var s2 = 'SELECT *\nFROM A\nWHERE A.a IN (SELECT foo FROM B);';
    test.deepEqual(Query.parseTableNames(s2), [['A', null], ['B', null]]);

    var s3 = 'SELECT A FROM T1 WHERE EXISTS (SELECT B FROM T2 WHERE T1.X = T2.Y)';
    test.deepEqual(Query.parseTableNames(s3), [['T1', null], ['T2', null]]);

    var s4 = 'SELECT col1 FROM (SELECT col1, SUM(col2) AS col2sum FROM t1 GROUP BY col1) t2 WHERE t2.col2sum > 10';
    test.deepEqual(Query.parseTableNames(s4), [['t1', null]]);

    test.done();
  },
  tearDown: function (callback) {
    // clean up
    callback();
  }
});
