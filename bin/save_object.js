var shib = require('shib'),
    servers = require('../config').servers;
shib.init(servers);

var client = shib.client();
var Query = require('shib/query').Query,
    Result = require('shib/result').Result;

var save_data = {
  query: "SELECT\n  hoge, pos\n  FROM moge_table WHERE key=__KEY__ AND query LIKE '%<script>alert(1)</script>%'",
  keywords: ['whatsup'],
  schema: [{name:'hoge', type:'string'}, {name:'pos', type:'bigint'}],
  executed_at: 'Tue Jun 17 2011 17:23:00 GMT+0900 (JST)',
  data: ["new1\t700","top\t500","img\t130","api\t93","css\t91","js\t90","touch\t51"],
  lines: 7,
  bytes: 55,
  error: null
};

/*
 executed_at
 Tue Jun 14 2011 19:11:05 GMT+0900 (JST)

 schema format
 [ { name: 'x', type: 'string', comment: null },
   { name: 'cnt', type: 'bigint', comment: null } ]

 data: list of tab-separated-values

var save_data = {
  query: 'SELECT date,count(date) AS cnt FROM testtable WHERE date LIKE "2011/05/%" SORT BY date',
  keywords: [],
  schema: [],
  executed_at: null,
  data: [],
  lines: 0,
  bytes: 0,
  error: null
};
*/

var query_string = save_data.query;
var query_keywords = save_data.keywords;
var result_schema = save_data.schema;
var result_executed_at = save_data.executed_at;
var result_data = save_data.data;
var result_lines = save_data.lines;
var result_bytes = save_data.bytes;
var result_error = save_data.error;

client.createQuery(query_string, query_keywords, function(err, query){
  client.addHistory(query);
  client.addKeyword(query);
  var result = new Result({queryid:query.queryid, executed_at:(result_executed_at || (new Date()).toLocaleString())});
  query.results.push({executed_at:result.executed_at, resultid:result.resultid});
  this.updateQuery(query, function(err){
    if (result_error && result_error !== '') {
      result.state = 'error';
      result.error = result_error;
    }
    else if (result_data.length == 0){
      result.state = 'running';
    }
    else {
      result.state = 'done';
      result.schema = result_schema;
      result.lines = result_lines;
      result.bytes = result_bytes;
    }
    this.setResult(result, function(e){
      this.appendResultData(result.resultid, result_data, function(e){
        console.log({query:query, result:result, result_data:result_data});
        process.exit();
      });
    });
  });
});
