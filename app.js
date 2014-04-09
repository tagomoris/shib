var express = require('express'),
    jade = require('jade'),
    async = require('async'),
    fs = require('fs'),
    app = express();

var RECENT_FETCHES = 50;
var SHOW_RESULT_HEAD_LINES = 20;

var InvalidQueryError = require('shib/query').InvalidQueryError;
var SimpleCSVBuilder = require('shib/simple_csv_builder').SimpleCSVBuilder;

var shib = require('shib'),
    servers = require('./config').servers;

if (process.env.NODE_ENV === 'production') {
  servers = require('./production').servers;
}
shib.init(servers);

function shutdown(signal){
  console.log((new Date()).toString() + ': Shutdown by signal, ' + signal);
  process.exit();
};
process.on('SIGINT', function(){ shutdown('SIGINT'); });
process.on('SIGHUP', function(){ shutdown('SIGHUP'); });
process.on('SIGQUIT', function(){ shutdown('SIGQUIT'); });
process.on('SIGTERM', function(){ shutdown('SIGTERM'); });

var runningQueries = {};

function error_handle(req, res, err){
  console.log(err);
  console.log(err.stack);
  res.send(err, 500);
};

app.configure(function(){
  app.use(express.logger('default'));
  app.use(express.methodOverride());
  app.use(express.urlencoded());
  app.use(express.json());
  app.use(express.bodyParser());

  app.use(app.router);

  app.set('view options', {layout: false});
  app.set('port', (servers.listen || process.env.PORT || 3000));
});

app.use(function(err, req, res, next){
  if (!err) {
    next();
  }
  else {
    console.log(err);
    if (err instanceof Object)
      res.send(500, JSON.stringify(err));
    else
      res.send(500, err);
  }
});

app.configure('development', function(){
  app.use(express.static(__dirname + '/public'));
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
});

app.configure('production', function(){
  var onehour = 3600;
  app.use(express.static(__dirname + '/public', { maxAge: onehour }));
  app.use(express.errorHandler());
});

app.get('/', function(req, res){
  var client = shib.client();
  res.render(__dirname + '/views/index.jade');
  client.end();
});

app.get('/q/:queryid', function(req, res){
  // Only this request handler is for permalink request from browser URL bar.
  var client = shib.client();
  res.render(__dirname + '/views/index.jade');
  client.end();
});

app.get('/t/:tag', function(req, res){
  // Only this request handler is for permalink request from browser URL bar.
  var client = shib.client();
  res.render(__dirname + '/views/index.jade');
  client.end();
});

app.get('/runnings', function(req, res){
  var runnings = [];
  for (var queryid in runningQueries) {
    var times = (function(){
      var secs = Math.floor(((new Date()) - runningQueries[queryid]) / 1000);
      if (secs < 60)
        return secs + ' seconds';
      var mins = Math.floor(secs / 60);
      if (mins < 60)
        return mins + ' minutes';
      var hours = Math.floor(mins / 60);
      return hours + ' hours';
    })();
    runnings.push([queryid, times]);
  }
  res.send(runnings);
});


var enginesCache = null;

app.get('/engines', function(req, res){
  /*
  // "monitor" means support 'status' (and 'kill') or not.
  {
    pairs: [ [engine_label, dbname], [engine_label, dbname], ... ],
    monitor: { label: bool }
  }
   */
  var info = enginesCache;
  if (info) {
    res.send(info);
  }
  else {
    shib.client().engineInfo(function(err, info){
      if (err) { error_handle(req, res, err); this.end(); return; }
      enginesCache = info;
      res.send(info);
      this.end();
    });
  }
});

app.get('/tables', function(req, res){
  var client = shib.client();
  var engineLabel = req.query.engine;
  var database = req.query.db;
  client.tables(engineLabel, database, function(err, result){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send(result);
    client.end();
  });
});

app.get('/partitions', function(req, res){
  var engineLabel = req.query.engine;
  var database = req.query.db;
  var tablename = req.query.key;
  if (/^[a-z0-9_]+$/i.exec(tablename) == null) {
    error_handle(req, res, {message: 'invalid tablename for show partitions: ' + tablename});
    return;
  }
  var client = shib.client();
  client.partitions(engineLabel, database, tablename, function(err, results){
    if (err) { error_handle(req, res, err); client.end(); return; }
    res.send(results);
    client.end();
  });
});

var describe_node_template = 'tr\n  td= colname\n  td= coltype\n  td= colcomment\n';

app.get('/describe', function(req, res){
  var engineLabel = req.query.engine;
  var database = req.query.db;
  var tablename = req.query.key;
  if (/^[a-z0-9_]+$/i.exec(tablename) == null) {
    error_handle(req, res, {message: 'invalid tablename for show partitions: ' + tablename});
    return;
  }
  var fn = jade.compile(describe_node_template);
  var client = shib.client();
  client.describe(engineLabel, database, tablename, function(err, result){
    if (err) { error_handle(req, res, err); client.end(); return; }
    var response_html = '<tr><th>col_name</th><th>type</th><th>comment</th></tr>';
    result.forEach(function(cols){
      response_html += fn.call(this, {colname: cols[0], coltype: cols[1], colcomment: cols[2]});
    });
    res.send([{title: '<table>' + response_html + '</table>'}]);
    client.end();
  });
});

app.get('/summary_bulk', function(req, res){
  var client = shib.client();

  var history_queries;
  var history = [];     /* ["201302", "201301", "201212", "201211"] */
  var history_ids = {}; /* {"201302":[query_ids], "201301":[query_ids], ...} */
  var query_ids = [];   /* [query_ids of all months] */
  var fetchRecent = function(cb){
    client.recentQueries(RECENT_FETCHES, function(err, list){ // list: [{yyyymm:...., queryid:....}]
      if (err) { cb(err); return; }
      history_queries = list;
      cb(null);
    });
  };
  var bundleMonths = function(cb){ // [{yyyymm:...,queryid:....}] => {yyyymm:[queryids], yyyymm:[queryids]}
    history_queries.forEach(function(row){
      var month = row.yyyymm;
      if (history.indexOf(month) < 0)
        history.push(month);
      if (! history_ids[month])
        history_ids[month] = [];
      history_ids[month].push(row.queryid);
      query_ids.push(row.queryid);
    });
    cb(null);
  };
  var queryUnique = function(cb){
    var exist_ids = {};
    query_ids = query_ids.filter(function(id){ if (exist_ids[id]) return false; exist_ids[id] = true; return true;});
    cb(null);
  };

  async.series([fetchRecent, bundleMonths, queryUnique], function(err, results){
    if (err) { error_handle(req, res, err); return; }
    res.send({history: history, history_ids: history_ids, query_ids: query_ids});
  });
});
  
app.post('/execute', function(req, res){
  var client = shib.client();
  var engineLabel = req.body.engineLabel;
  var dbname = req.body.dbname;
  if (!engineLabel || !dbname) {
    engineLabel = enginesCache.pairs[0][0];
    dbname = enginesCache.pairs[0][1];
  }

  var query = req.body.querystring;
  var scheduled = req.body.scheduled;
  client.createQuery(engineLabel, dbname, query, function(err, query){
    if (err) {
      if (err.error) {
        err = err.error;
      }
      if (err instanceof InvalidQueryError) {
        res.send(err, 400);
        this.end();
        return;
      }
      error_handle(req, res, err); return;
    }
    res.send(query);
    var queryid = query.queryid;
    this.execute(query, {
      scheduled: scheduled,
      prepare: function(){ runningQueries[queryid] = new Date(); },
      stopCheck: function(){ return (! runningQueries[queryid]); },
      stop:    function(){ client.end(); },
      success: function(){ delete runningQueries[queryid]; client.end(); },
      error:   function(){ delete runningQueries[queryid]; client.end(); }
    });
  });
});

app.post('/giveup', function(req, res){
  var targetid = req.body.queryid;
  var client = shib.client();
  client.query(targetid, function(err, query){
    client.giveup(query, function(err, query) {
      delete runningQueries[query.queryid];
      res.send(query);
      client.end();
    });
  });
});

app.post('/delete', function(req, res){
  var targetid = req.body.queryid;
  var client = shib.client();
  client.deleteRecent(targetid, function(err){
    if (err) {error_handle(req, res, err); client.end(); return;}
    delete runningQueries[targetid];
    client.deleteQuery(targetid, function(err){
      if (err) {error_handle(req, res, err); client.end(); return;}
      res.send({result:'ok'});
      client.end();
    });
  });
});

app.get('/query/:queryid', function(req, res){
  shib.client().query(req.params.queryid, function(err, query){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send(query);
    this.end();
  });
});

app.post('/queries', function(req, res){
  shib.client().queries(req.body.ids, function(err, queries){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send({queries:queries});
    this.end();
  });
});

app.get('/tags/:queryid', function(req, res){
  shib.client().tags(req.params.queryid, function(err, tags){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send(tags);
    this.end();
  });
});

app.post('/addtag', function(req, res){
  var tag = req.body.tag;
  if (tag.length < 1 || tag.length > 16) {
    error_handle(req, res, {message: 'invalid tag length [1-16]'});
    return;
  }
  shib.client().addTag(req.body.queryid, tag, function(err){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send({result:'ok'});
    this.end();
  });
});

app.post('/deletetag', function(req, res){
  shib.client().deleteTag(req.body.queryid, req.body.tag, function(err){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send({result:'ok'});
    this.end();
  });
});

app.get('/tagged/:tag', function(req, res){
  shib.client().taggedQueries(req.params.tag, function(err, queryids){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send(queryids);
    this.end();
  });
});

app.get('/taglist', function(req, res){
  shib.client().tagList(function(err, tags){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send(tags);
    this.end();
  });
});

app.get('/status/:queryid', function(req, res){
  shib.client().query(req.params.queryid, function(err, query){
    if (err) { error_handle(req, res, err); this.end(); return; }
    this.status(query, function(state){
      res.send(state);
      this.end();
    });
  });
});

app.get('/detailstatus/:queryid', function(req, res){
  shib.client().query(req.params.queryid, function(err, query){
    if (err) { error_handle(req, res, err); this.end(); return; }
    this.detailStatus(query, function(err, data){
      if (err) { error_handle(req, res, err); this.end(); return; }
      if (data === null)
        res.send('query not found', 404);
      else
        res.send(data);
      this.end();
    });
  });
});

app.get('/lastresult/:queryid', function(req, res){
  shib.client().query(req.params.queryid, function(err, query){
    if (err) { error_handle(req, res, err); this.end(); return; }
    if (query === null) {
      res.send('query not found', 404);
      this.end();
      return;
    }
    this.getLastResult(query, function(err, result){
      if (err) { error_handle(req, res, err); this.end(); return; }
      if (result === null)
        res.send('result not found', 404);
      else
        res.send(result);
      this.end();
    });
  });
});

app.get('/result/:resultid', function(req, res){
  shib.client().result(req.params.resultid, function(err, result){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send(result);
    this.end();
  });
});

app.post('/results', function(req, res){
  shib.client().results(req.body.ids, function(err, results){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send({results: results});
    this.end();
  });
});

app.get('/show/full/:resultid', function(req, res){
  var client = shib.client();
  var file = client.generatePath(req.params.resultid);
  if(!fs.existsSync(file)){
    res.send(null);
    res.end();
    client.end();
    return;
  }
  var rStream = fs.createReadStream(file);
  rStream.on('data', function(chunk) {
    rStream.pause();
    process.nextTick(function(){
      res.write(chunk);
    });
  });
  rStream.on('end', function(){
    res.end();
    client.end();
  });
  res.on('drain', function(){
    rStream.resume();
  });
});

app.get('/show/head/:resultid', function(req, res){
  var client = shib.client();
  var file = client.generatePath(req.params.resultid);
  if(!fs.existsSync(file)){
    res.send(null);
    res.end();
    client.end();
    return;
  }
  var rStream = fs.createReadStream(file);
  var readline = require('readline');
  var rl = readline.createInterface(rStream, {});
  var line_number = 0;
  rl.on('line', function(line) {
    if(line_number < SHOW_RESULT_HEAD_LINES){
      res.write(line + '\n');
      line_number++;
    }else{
      rl.close();
     }
  });
  rl.on('close', function() {
    res.end();
    client.end();
  });
  res.on('resume', function() {
    rl.resume();
  });
});

app.get('/download/tsv/:resultid', function(req, res){
  shib.client().result(req.params.resultid, function(err, result){
    if (err) { error_handle(req, res, err); this.end(); return; }

    res.attachment(req.params.resultid + '.tsv');
    res.set('X-Shib-Query-ID', result.queryid);
    res.set('X-Shib-Result-ID', result.resultid);
    res.set('X-Shib-Executed-At', result.executed_msec || 0);
    res.set('X-Shib-Completed-At', result.completed_msec || 0);

    var client = shib.client();
    var file = client.generatePath(req.params.resultid);
    if(!fs.existsSync(file)){
      res.send(null);
      res.end();
      client.end();
      return;
    }
    var rStream = fs.createReadStream(file);
    rStream.on('data', function(chunk){
      rStream.pause();
      process.nextTick(function(){
        res.write(chunk);
      });
    });
    rStream.on('end', function(){
      res.end();
      client.end();
    });
    res.on('drain', function(){
      rStream.resume();
    });

  });
});

app.get('/download/csv/:resultid', function(req, res){
  shib.client().result(req.params.resultid, function(err, result){
    if (err) { error_handle(req, res, err); this.end(); return; }

    res.attachment(req.params.resultid + '.csv');
    res.set('X-Shib-Query-ID', result.queryid);
    res.set('X-Shib-Result-ID', result.resultid);
    res.set('X-Shib-Executed-At', result.executed_msec || 0);
    res.set('X-Shib-Completed-At', result.completed_msec || 0);

    var client = shib.client();
    var file = client.generatePath(req.params.resultid);
    if(!fs.existsSync(file)){
      res.send(null);
      res.end();
      client.end();
      return;
    }
    var rStream = fs.createReadStream(file);
    var readline = require('readline');
    var rl = readline.createInterface(rStream, {});
    rl.on('line', function(line){
      res.write(SimpleCSVBuilder.build(line.split('\t')));
    });
    rl.on('close', function(){
      res.end();
      client.end();
    });
    res.on('resume', function(){
      rl.resume();
    });

  });
});

shib.client().end(); // to initialize sqlite3 database

// var enginesCache = null;
var enginesCacheUpdate = function(){
  shib.client().engineInfo(function(err, info){
    if (!err && info)
      enginesCache = info;
    this.end();
  });
};
setInterval(enginesCacheUpdate, 60*60*1000); // cache update per hour

console.log((new Date()).toString() + ': Starting shib.');

enginesCacheUpdate();

app.listen(app.get('port'));
