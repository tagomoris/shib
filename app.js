var express = require('express'),
    jade = require('jade'),
    async = require('async'),
    app = express.createServer();

var MAX_ACCORDION_SIZE = 20;

var shib = require('shib'),
    servers = require('./config').servers;
var InvalidQueryError = require('shib/query').InvalidQueryError;
shib.init(servers);

function error_handle(req, res, err){
  //TODO make log line
  console.log(err);
  console.log(err.stack);
  //console.log({time: (new Date()), request:req, error:err});
  res.send(err, 500);
};

app.configure(function(){
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
    this.execute(query);
  });
});

app.post('/refresh', function(req, res){
  shib.client().query(req.body.queryid, function(err, query){
    if (err) { error_handle(req, res, err); return; }
    this.refresh(query);
    res.send('ok');
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
  shib.client().getQuery(req.params.queryid, function(err, query){
    if (err) { error_handle(req, res, err); return; }
    res.send(query); /* *** */
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
  /* */
});

app.get('/result/:resultid', function(req, res){
  /* */
  shib.client().getQuery();
});

app.post('/results', function(req, res){
  shib.client().results(req.body.ids, function(err, results){
    if (err) { error_handle(req, res, err); return; }
    res.send({results: results});
  });
});

app.get('/rawresult/:resultid', function(req, res){
  /* */
});

app.listen(3000);