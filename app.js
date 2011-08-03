var express = require('express'),
    jade = require('jade'),
    async = require('async'),
    app = express.createServer();

var MAX_ACCORDION_SIZE = 20;
var SHOW_RESULT_HEAD_LINES = 20;

var InvalidQueryError = require('shib/query').InvalidQueryError;
var SimpleCSVBuilder = require('shib/simple_csv_builder').SimpleCSVBuilder;

var shib = require('shib'),
    servers = require('./config').servers;
shib.init(servers);

function error_handle(req, res, err){
  //TODO make log line
  console.log(err);
  console.log(err.stack);
  //console.log({time: (new Date()), request:req, error:err});
  res.send(err, 500);
};

app.configure(function(){
  app.use(express.logger({ format: ':method :url' }));
  app.use(express.methodOverride());
  app.use(express.bodyParser());
  app.use(app.router);
});

app.configure('development', function(){
  app.use(express.static(__dirname + '/public'));
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
});

app.configure('production', function(){
  var oneYear = 31557600000;
  app.use(express.static(__dirname + '/public', { maxAge: oneYear }));
  app.use(express.errorHandler());
});

app.get('/', function(req, res){
  // res.redirect('/index.html');
  res.render(__dirname + '/views/index.jade', {layout: false});
});

app.get('/q/:queryid', function(req, res){
  // Only this request handler is for permalink request from browser URL bar.
  res.render(__dirname + '/views/index.jade', {layout: false});
});

app.get('/tables', function(req, res){
  shib.client().executeSystemStatement('show tables', function(err, result){
    if (err) { error_handle(req, res, err); return; }
    res.send(result);
  });
});

app.get('/partitions', function(req, res){
  var tablename = req.query.key;
  if (/^[a-z0-9_]+$/i.exec(tablename) == null) {
    error_handle(req, res, {message: 'invalid tablename for show partitions: ' + tablename});
    return;
  }
  shib.client().executeSystemStatement('show partitions ' + tablename, function(err, result){
    if (err) { error_handle(req, res, err); return; }
    var response_obj = [];
    var treenodes = {};
    
    var create_node = function(partition, hasChildren){
      if (treenodes[partition])
        return treenodes[partition];
      var parts = partition.split('/');
      var leafName = parts.pop();
      var node = {title: leafName};
      if (hasChildren) {
        node.children = [];
      }
      if (parts.length > 0) {
        var parent = create_node(parts.join('/'), true);
        parent.children.push(node);
      }
      else {
        response_obj.push(node);
      }
      treenodes[partition] = node;
      return node;
    };

    result.forEach(function(partition){
      create_node(partition);
    });
    res.send(response_obj);
  });
});

var describe_node_template = 'tr\n  td= colname\n  td= coltype\n  td= colcomment\n';

app.get('/describe', function(req, res){
  var tablename = req.query.key;
  if (/^[a-z0-9_]+$/i.exec(tablename) == null) {
    error_handle(req, res, {message: 'invalid tablename for show partitions: ' + tablename});
    return;
  }
  var fn = jade.compile(describe_node_template);
  shib.client().executeSystemStatement('describe ' + tablename, function(err, result){
    if (err) { error_handle(req, res, err); return; }
    var response_title = '<tr><th>col_name</th><th>type</th><th>comment</th></tr>';
    result.forEach(function(row){
      var cols = row.split('\t');
      response_title += fn.call(this, {colname: cols[0], coltype: cols[1], colcomment: cols[2]});
    });
    res.send([{title: '<table>' + response_title + '</table>'}]);
  });
});

app.get('/summary_bulk', function(req, res){
  var correct_history = function(callback){
    shib.client().getHistories(function(err, list){
      if (err) {callback(err); return;}
      this.getHistoryBulk(list, function(err, idlist){
        if (err) {callback(err); return;}
        var idmap = {};
        var ids = [];
        for (var x = 0; x < list.length; x++) {
          idmap[list[x]] = idlist[x].slice(0,MAX_ACCORDION_SIZE).reverse();
          ids = ids.concat(idmap[list[x]]);
        }
        callback(null, {history:list.reverse(), history_ids:idmap, ids:ids});
      });
    });
  };
  var correct_keywords = function(callback){
    shib.client().getKeywords(function(err, list){
      if (err) {callback(err); return;}
      this.getKeywordBulk(list, function(err, idlist){
        if (err) {callback(err); return;}
        var idmap = {};
        var ids = [];
        for (var y = 0; y < list.length; y++) {
          idmap[list[y]] = idlist[y].slice(0,MAX_ACCORDION_SIZE).reverse();
          ids = ids.concat(idmap[list[y]]);
        }
        callback(null, {keywords:list, keyword_ids:idmap, ids:ids});
      });
    });
  };

  async.parallel([correct_history, correct_keywords], function(err, results){
    if (err) {
      error_handle(req, res, err);
      return;
    }
    var response_obj = {
      history: (results[0].history || results[1].history),
      history_ids: (results[0].history_ids || results[1].history_ids),
      keywords: (results[0].keywords || results[1].keywords),
      keyword_ids: (results[0].keyword_ids || results[1].keyword_ids)
    };
    var exist_ids = {};
    response_obj.query_ids = results[0].ids.concat(results[1].ids).filter(function(v){
      if (exist_ids[v]) return false;
      exist_ids[v] = true;
      return true;
    });
    res.send(response_obj);
  });
});
  
app.post('/execute', function(req, res){
  var keywords = req.body.keywords;
  shib.client().createQuery(req.body.querystring, keywords, function(err, query){
    if (err) {
      if (err instanceof InvalidQueryError) {
        res.send(err, 400);
        return;
      }
      error_handle(req, res, err); return;
    }
    res.send(query);
    this.execute(query, (query.results.length > 0)); // refreshed execution or not
  });
});

app.post('/delete', function(req, res){
  var targetid = req.body.queryid;
  var targetKeyword = req.body.keyword;
  var targetHistorySize = 5;

  shib.client().getHistories(function(err, histories){
    if (err)
      histories = [];
    var client = this;
    var targetHistories = histories.sort().reverse().slice(0, targetHistorySize); // unti-dictionary order, last 'targetHistorySize' items
    var funclist = [
      function(callback){client.removeKeyword(targetKeyword, targetid); callback(null, 1);},
      function(callback){client.deleteQuery(targetid); callback(null, 1);}
    ].concat(targetHistories.map(function(h){return function(callback){client.removeHistory(h, targetid); callback(null, 1);};}));
    async.parallel(funclist, function(err, results){
      if (err) {error_handle(req, res, err); return;}
      res.send({result:'ok'});
    });
  });
});

app.post('/refresh', function(req, res){
  shib.client().query(req.body.queryid, function(err, query){
    if (err) { error_handle(req, res, err); return; }
    this.refresh(query);
    res.send({result:'ok'});
  });
});

app.get('/keywords', function(req, res){
  shib.client().getKeywords(function(err, keywords){
    if (err) { error_handle(req, res, err); return; }
    res.send(keywords); /* **** */
  });
});

app.get('/keyword/:label', function(req, res){
  shib.client().getKeyword(req.params.label, function(err, idlist){
    if (err) { error_handle(req, res, err); return; }
    res.send(idlist); /* **** */
  });
});

app.get('/histories', function(req, res){
  shib.client().getHistories(function(err, histories){
    if (err) { error_handle(req, res, err); return; }
    res.send(histories); /* *** */
  });
});

app.get('/history/:label', function(req, res){
  shib.client().getHistory(req.params.label, function(err, idlist){
    if (err) { error_handle(req, res, err); return; }
    res.send(idlist); /* **** */
  });
});

app.get('/query/:queryid', function(req, res){
  shib.client().query(req.params.queryid, function(err, query){
    if (err) { error_handle(req, res, err); return; }
    res.send(query);
  });
});

app.post('/queries', function(req, res){
  shib.client().queries(req.body.ids, function(err, queries){
    if (err) { error_handle(req, res, err); return; }
    res.send({queries: queries});
  });
});

app.get('/status/:queryid', function(req, res){
  shib.client().query(req.params.queryid, function(err, query){
    if (err) { error_handle(req, res, err); return; }
    this.status(query, function(state){
      res.send(state);
    });
  });
});

app.get('/lastresult/:queryid', function(req, res){
  shib.client().query(req.params.queryid, function(err, query){
    if (err) { error_handle(req, res, err); return; }
    this.getLastResult(query, function(err, result){
      if (err) { error_handle(req, res, err); return; }
      res.send(result);
    });
  });
});

app.get('/result/:resultid', function(req, res){
  shib.client().result(req.params.resultid, function(err, result){
    if (err) { error_handle(req, res, err); return; }
    res.send(result);
  });
});

app.post('/results', function(req, res){
  shib.client().results(req.body.ids, function(err, results){
    if (err) { error_handle(req, res, err); return; }
    res.send({results: results});
  });
});

app.get('/show/full/:resultid', function(req, res){
  shib.client().rawResultData(req.params.resultid, function(err, data){
    if (err) { error_handle(req, res, err); return; }
    res.send(data);
  });
});
app.get('/show/head/:resultid', function(req, res){
  shib.client().rawResultData(req.params.resultid, function(err, data){
    if (err) { error_handle(req, res, err); return; }
    var headdata = [];
    var counts = 0;
    var position = 0;
    while (counts < SHOW_RESULT_HEAD_LINES && position < data.length) {
      var nextNewline = data.indexOf('\n', position);
      if (nextNewline < 0) {
        headdata.push(data.substring(position));
        counts += 1;
        position = data.length;
      }
      else{
        headdata.push(data.substring(position, nextNewline + 1));
        counts += 1;
        position = nextNewline + 1;
      }
    }
    res.send(headdata.join(''));
  });
});
app.get('/download/tsv/:resultid', function(req, res){
  shib.client().rawResultData(req.params.resultid, function(err, data){
    if (err) { error_handle(req, res, err); return; }
    res.attachment(req.params.resultid + '.tsv');
    res.send(data);
  });
});
app.get('/download/csv/:resultid', function(req, res){
  shib.client().rawResultData(req.params.resultid, function(err, data){
    if (err) { error_handle(req, res, err); return; }
    res.attachment(req.params.resultid + '.csv');
    var rows = data.split("\n");
    if (rows[rows.length - 1].length < 1)
      rows.pop();
    res.send(rows.map(function(row){return SimpleCSVBuilder.build(row.split('\t'));}).join(''));
  });
});

app.listen(3000);
