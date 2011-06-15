var express = require('express'),
    jade = require('jade'),
    app = express.createServer();

var shib = require('shib'),
    servers = require('./config').servers;
shib.init(servers);

var complete_callback = function(list, callback){
  var waiting_callbacks = list;
  var errors = [];
  var position = {};
  waiting_callbacks.forEach(function(k){position[k] = false;});
  return function(type, err){
    if (err)
      errors.push(err);
    position[type] = true;
    for(var i = 0; i < waiting_callbacks.length; i++) {
      if (! position[waiting_callbacks[i]])
        return;
    }
    if (errors.length > 0)
      callback(errors);
    else
      callback();
  };
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

//TODO: set error status into error handler responses

app.get('/summary_bulk', function(req, res){
  // keyword-list, keyword-ids list, history, yyyymm-ids list, uniqueue query id list
  var history = null;
  var history_ids = {};
  var keywords = null;
  var keyword_ids = {};
  var summary_bulk_callback = complete_callback(['history_idlist', 'keyword_idlist'], function(errors){
    // create query_id_unique list
    var response_obj = {
      history: history,
      history_ids: history_ids,
      keywords: keywords,
      keyword_ids: keyword_ids,
      query_ids: query_ids
    };
    //TODO: if errors && errors.length > 0 ...
    res.send(response_obj); // serialize!
  });
  shib.client().getHistories(function(err, list){
    if (err) {summary_bulk_callback('history_idlist', err); return;}
    history = list;
    this.getHistoryBulk(list, function(err, idlist){
      if (err) {summary_bulk_callback('history_idlist', err); return;}
      for (var x = 0; x < history.length; x++) {
        history_ids[history[x]] = idlist[x];
      }
      summary_bulk_callback('history_idlist');
    });
  });
  shib.client().getKeywords(function(err, list){
    if (err) {summary_bulk_callback('keyword_idlist', err); return;}
    keywords = list;
    this.getKeywordBulk(list, function(err, idlist){
      if (err) {summary_bulk_callback('keyword_idlist', err); return;}
      for (var y = 0; y < keywords.length; y++) {
        keyword_ids[keywords[y]] = idlist[y];
      }
      summary_bulk_callback('keyword_idlist');
    });
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