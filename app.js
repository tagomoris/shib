var express = require('express'),
    app = express.createServer();

var config = {
  hiveserver: {
    host: 'localhost',
    port: 10000
  },
  kyototycoon: {
    host: 'localhost',
    port: 3000 // dakke?
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
  res.redirect('/index.html');
});

app.post('/execute', function(req, res){
  shib.client().createQuery(req.querystring, function(err, data){
    //TODO: double-executed registration
    res.send(data.queryid);
    this.execute(data);
  });
});

app.get('/refresh/:id', function(req, res){
  shib.client();
});

app.get('/status/:id', function(req, res){
  shib.client().getQuery(req.params.id, function(data){
    if (data) {
      res.send(data);
      this.end();
      return;
    }
    res.set('30x/404/...');
    this.end();
  });
});

app.get('/result/:id', function(req, res){
  shib.client().getQuery();
});

app.listen(3000);