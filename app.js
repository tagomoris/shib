var express = require('express'),
    jade = require('jade'),
    async = require('async'),
    app = express.createServer();

var shib = require('shib'),
    servers = require('./config').servers;
shib.init(servers);

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

//TODO: set error status into error handler responses

app.get('/summary_bulk', function(req, res){
  var correct_history = function(callback){
    shib.client().getHistories(function(err, list){
      if (err) {callback(err); return;}
      this.getHistoryBulk(list, function(err, idlist){
        if (err) {callback(err); return;}
        var idmap = {};
        var ids = [];
        for (var x = 0; x < list.length; x++) {
          idmap[list[x]] = idlist[x];
          ids.concat(idlist[x]);
        }
        callback(null, {history:list, history_ids:idmap, ids:ids});
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
          idmap[list[y]] = idlist[y];
          ids.concat(idlist[y]);
        }
        callback(null, {keywords:list, keyword_ids:idmap, ids:ids});
      });
    });
  };

  async.parallel([correct_history, correct_keywords], function(err, results){
    if (err) {
      //TODO: error handle...
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
    res.send(response_obj); // serialize!
  });
});
  
app.post('/execute', function(req, res){
  var keywords = req.keywords.split(',');
  shib.client().createQuery(req.querystring, keywords, function(err, query){
    if (err) {res.send(err); return;}
    res.send(query.queryid);
    this.execute(query);
  });
});

app.post('/refresh', function(req, res){
  shib.client().query(req.queryid, function(err, query){
    if (err) {res.send(err); return;}
    this.refresh(query);
  });
});

app.get('/keywords', function(req, res){
  shib.client().getKeywords(function(err, keywords){
    if (err) {res.send(err); return;}
    res.send(keywords); /* **** */
  });
});

app.get('/keyword/:label', function(req, res){
  shib.client().getKeyword(req.params.label, function(err, idlist){
    if (err) {res.send(err); return;}
    res.send(idlist); /* **** */
  });
});

app.get('/histories', function(req, res){
  shib.client().getHistories(function(err, histories){
    if (err) {res.send(err); return;}
    res.send(histories); /* *** */
  });
});

app.get('/history/:label', function(req, res){
  shib.client().getHistory(req.params.label, function(err, idlist){
    if (err) {res.send(err); return;}
    res.send(idlist); /* **** */
  });
});

app.get('/query/:queryid', function(req, res){
  shib.client().getQuery(req.params.queryid, function(err, query){
    if (err) {res.send(err); return;}
    res.send(query); /* *** */
  });
});

app.post('/queries', function(req, res){
  /* */
});

app.get('/status/:queryid', function(req, res){
  shib.client().query(req.params.queryid, function(err, query){
    //error...
    res.set('30x/404/...');
    this.status(query, function(state){
      //if state ....
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

app.get('/rawresult/:resultid', function(req, res){
  /* */
});

app.listen(3000);