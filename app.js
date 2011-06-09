var express = require('express'),
    jade = require('jade'),
    app = express.createServer();

var config = {
  hiveserver: {
    host: 'localhost',
    port: 10000
  },
  kyototycoon: {
    host: 'localhost',
    port: 1978
  }
};

var shib = require('shib');
shib.init(config);

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

app.post('/execute', function(req, res){
  shib.client().createQuery(req.querystring, req.keywords, function(err, query){
    res.send(query.queryid);
    this.execute(query);
  });
});

app.post('/refresh', function(req, res){
  shib.client().query(req.queryid, function(err, query){
    //error...
    this.refresh(query);
  });
});

app.get('/keywords', function(req, res){
  /* */
});

app.get('/keyword/:label', function(req, res){
  /* */
});

app.get('/histories', function(req, res){
  /* */
});

app.get('/history/:label', function(req, res){
  /* */
});

app.get('/query/:queryid', function(req, res){
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
  shib.client().getQuery();
});

app.get('/rawresult/:resultid', function(req, res){
  /* */
});

app.listen(3000);