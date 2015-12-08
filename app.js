var express = require('express'),
    jade = require('jade'),
    async = require('async'),
    fs = require('fs'),
    app = express();

var RECENT_FETCHES = 50;
var SHOW_RESULT_HEAD_LINES = 20;

var InvalidQueryError = require('shib/query').InvalidQueryError;
var SimpleCSVBuilder = require('shib/simple_csv_builder').SimpleCSVBuilder;

var shib = require('shib');
var config_package = {};

if (process.env.NODE_ENV) {
  config_package = require('./' + process.env.NODE_ENV);
} else {
  config_package = require('./config');
}
var servers = config_package.servers;
shib.init(servers);

function shutdown(signal){
  shib.logger().info('Shutdown by signal', {signal: signal});
  process.exit();
};
process.on('SIGINT', function(){ shutdown('SIGINT'); });
process.on('SIGHUP', function(){ shutdown('SIGHUP'); });
process.on('SIGQUIT', function(){ shutdown('SIGQUIT'); });
process.on('SIGTERM', function(){ shutdown('SIGTERM'); });

var runningQueries = {};

function error_handle(req, res, err){
  shib.logger().error('Error in app', err);
  if (err.stack)
    shib.logger().error(err.stack);
  res.send(err, 500);
};

function shibclient(req){
  return shib.client({credential: shib.auth().credential(req)});
}

app.configure(function(){
  app.use(express.static(__dirname + '/public'));
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
    shib.logger().error('ServerError', err);
    err.stack.split("\n").forEach(function(line){
      shib.logger().error(line);
    });
    res.send(500, 'Server Error');
  }
});

app.configure('development', function(){
  app.use(express.static(__dirname + '/public'));
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
});

app.configure('production', function(){
  app.use(express.logger('default'));
  var onehour = 3600;
  app.use(express.static(__dirname + '/public', { maxAge: onehour }));
  app.use(express.errorHandler());
});

app.get('/', function(req, res){
  var client = shibclient(req);
  res.render(__dirname + '/views/index.jade');
  client.end();
});

app.post('/auth', function(req, res){
  var auth = shib.auth();

  auth.check(req, function(err, result){
    if (err) { error_handle(req, res, err); return; }
    if (result) {
      // if auth module is dumb, username becomes String('undefined') and password becomes String('undefined')
      var authInfo = auth.crypto(result.username);
      res.send(200, {authInfo: authInfo, realm: auth.realm, enabled: auth.enabled});
    } else {
      res.send(403, {authInfo: null, realm: auth.realm, enabled: auth.enabled});
    }
  });
});

app.get('/q/:queryid', function(req, res){
  // Only this request handler is for permalink request from browser URL bar.
  var client = shibclient(req);
  res.render(__dirname + '/views/index.jade');
  client.end();
});

app.get('/t/:tag', function(req, res){
  // Only this request handler is for permalink request from browser URL bar.
  var client = shibclient(req);
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

  // If authentications are always required, database list cannot be cached.
  if (shib.auth().require_always) {
    shibclient(req).engineInfo(function(err, info){
      if (err) { error_handle(req, res, err); this.end(); return; }
      res.send(info);
      this.end();
    });
    return;
  }

  var info = enginesCache;
  if (info) {
    res.send(info);
  }
  else {
    shibclient(req).engineInfo(function(err, info){
      if (err) { error_handle(req, res, err); this.end(); return; }
      enginesCache = info;
      res.send(info);
      this.end();
    });
  }
});

app.get('/tables', function(req, res){
  var client = shibclient(req);
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
  var client = shibclient(req);
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
  var client = shibclient(req);
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
  var client = shibclient(req);

  if (shib.setting('disable_history')) {
    res.send({disabled: true});
    return;
  }

  var history_queries;
  var history = [];     /* ["201302", "201301", "201212", "201211"] */
  var history_ids = {}; /* {"201302":[query_ids], "201301":[query_ids], ...} */
  var query_ids = [];   /* [query_ids of all months] */
  var fetchRecent = function(cb){
    client.recentQueries(RECENT_FETCHES, function(err, list){ // list: [Query, ...]
      if (err) { cb(err); return; }
      history_queries = list;
      cb(null);
    });
  };
  var bundleMonths = function(cb){ // [{yyyymm:...,queryid:....}] => {yyyymm:[queryids], yyyymm:[queryids]}
    var pad = function(n){return n < 10 ? '0'+n : n;};
    var historyKey = function(date){
      return '' + date.getFullYear() + pad(date.getMonth() + 1);
    };
    history_queries.forEach(function(query){
      var month = historyKey(query.datetime);
      if (history.indexOf(month) < 0)
        history.push(month);
      if (! history_ids[month])
        history_ids[month] = [];
      history_ids[month].push(query.queryid);
      query_ids.push(query.queryid);
    });
    history.sort().reverse();
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

// generate pseudo query object (simulate v0 query)
function pseudo_query_data(query){
  var q = {};
  q['queryid'] = query.queryid;
  q['engine'] = query.engine;
  q['dbname'] = query.dbname;
  q['querystring'] = query.querystring;
  q['results'] = [];
  if (query.state !== "running") {
    q['results'] = [{resultid: query.resultid, executed_at: new Date(query.datetime).toLocaleString()}];
  }
  return q;
}

app.post('/execute', function(req, res){
  var auth = shib.auth();

  var client = shibclient(req);
  var engineLabel = req.body.engineLabel;
  var dbname = req.body.dbname;
  if (!engineLabel || !dbname) {
    engineLabel = enginesCache.pairs[0][0];
    dbname = enginesCache.pairs[0][1];
  }

  var querystring = req.body.querystring;
  var scheduled = req.body.scheduled;

  var userdata = auth.userdata(req);

  if (auth.require_always && !userdata) {
    shib.logger().warn('This shib requires authentication to execute queries, but there are no authInfo', {query: querystring});
    res.send(403, "Authentication failed");
    return;
  }
  if (userdata) {
    shib.logger().info('User try to execute query', {username: userdata.username, query: querystring});
  }

  client.createQuery(engineLabel, dbname, querystring, scheduled, function(err, query){
    if (err) {
      if (err.error) {
        err = err.error;
      }
      if (err instanceof InvalidQueryError) {
        shib.logger().warn('Invalid query submitted', {query: querystring, err: err.message});
        res.send(400, err.message);
        this.end();
        return;
      }
      error_handle(req, res, err); return;
    }
    res.send(pseudo_query_data(query));
    var queryid = query.queryid;
    this.execute(query, {
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
  var client = shibclient(req);
  client.query(targetid, function(err, query){
    client.giveup(query, function(err, query) {
      if (err) {error_handle(req, res, err); client.end(); return;}
      client.end(true); // half close
      delete runningQueries[query.queryid];
      res.send(pseudo_query_data(query));
    });
  });
});

app.post('/delete', function(req, res){
  var targetid = req.body.queryid;
  var client = shibclient(req);
  delete runningQueries[targetid];
  client.deleteQuery(targetid, function(err){
    if (err) {error_handle(req, res, err); client.end(); return;}
    res.send({result:'ok'});
    client.end();
  });
});

//TODO: any APIs w/o pseudo_query_data ?

app.get('/query/:queryid', function(req, res){
  shibclient(req).query(req.params.queryid, function(err, query){
    if (err) { error_handle(req, res, err); this.end(); return; }
    if (query === null) {
      res.send('query not found', 404);
      this.end();
      return;
    }
    res.send(pseudo_query_data(query));
    this.end();
  });
});

app.post('/queries', function(req, res){
  shibclient(req).queries(req.body.ids, function(err, queries){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send({queries: queries.map(function(q){ return pseudo_query_data(q); })});
    this.end();
  });
});

app.get('/tags/:queryid', function(req, res){
  shibclient(req).tags(req.params.queryid, function(err, tags){
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
  shibclient(req).addTag(req.body.queryid, tag, function(err){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send({result:'ok'});
    this.end();
  });
});

app.post('/deletetag', function(req, res){
  shibclient(req).deleteTag(req.body.queryid, req.body.tag, function(err){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send({result:'ok'});
    this.end();
  });
});

app.get('/tagged/:tag', function(req, res){
  shibclient(req).taggedQueries(req.params.tag, function(err, queryids){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send(queryids);
    this.end();
  });
});

app.get('/taglist', function(req, res){
  shibclient(req).tagList(function(err, tags){
    if (err) { error_handle(req, res, err); this.end(); return; }
    res.send(tags);
    this.end();
  });
});

// convert query.state into status label (simulate v0 Client.prototype.status)
function pseudo_status(query_state){
   /*
    running: newest-and-only query running, and result not stored yet.
    executed (done): newest query executed, and result stored.
    error: newest query executed, but done with error.
   */
  switch (query_state) {
    case 'running': return "running";
    case 'error': return "error";
    default: return "executed";
  }
}

app.get('/status/:queryid', function(req, res){
  shibclient(req).query(req.params.queryid, function(err, query){
    if (err) { error_handle(req, res, err); this.end(); return; }
    if (query === null) {
      res.send('query not found', 404);
      this.end();
      return;
    }
    res.send(pseudo_status(query.state));
    this.end();
  });
});

app.get('/detailstatus/:queryid', function(req, res){
  shibclient(req).query(req.params.queryid, function(err, query){
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

// generate pseudo result object (simulate v0 result)
function pseudo_result_data(query){
  var r = query.result;
  r['resultid'] = query.resultid;
  r['executed_at'] = new Date(query.datetime).toLocaleString();
  if (r['completed_at'])
    r['completed_at'] = new Date(r['completed_at']).toLocaleString();
  r['state'] = query.state;
  return r;
}

app.get('/lastresult/:queryid', function(req, res){
  shibclient(req).query(req.params.queryid, function(err, query){
    if (err) { error_handle(req, res, err); this.end(); return; }
    if (query === null) {
      res.send('query not found', 404);
      this.end();
      return;
    }
    res.send(pseudo_result_data(query));
    this.end();
  });
});

app.get('/result/:resultid', function(req, res){
  shibclient(req).getQueryByResultId(req.params.resultid, function(err, query){
    if (err) { error_handle(req, res, err); this.end(); return; }
    if (query === null) {
      res.send('query not found', 404);
      this.end();
      return;
    }
    res.send(pseudo_result_data(query));
    this.end();
  });
});

app.post('/results', function(req, res){
  /*
   * obsolete: bad implementation for API compatibility
   */
  var client = shibclient(req);
  var fetchers = (req.body.ids || []).map(function(resultid){
    return function(cb){
      client.getQueryByResultId(resultid, function(err, query){
        if (query === null) {
          res.send('query not found', 404);
          this.end();
          return;
        }
        if (err) { cb(err); return; }
        cb(null, pseudo_result_data(query));
      });
    };
  });
  async.series(fetchers, function(err, results){
    if (err) { error_handle(req, res, err); client.end(); return; }
    res.send({results: results});
    client.end();
  });
});

app.get('/show/full/:resultid', function(req, res){
  var client = shibclient(req);
  var file = client.generatePath(req.params.resultid);
  if (! fs.existsSync(file)) {
    res.send(null);
    res.end();
    client.end();
    return;
  }
  res.sendfile(file)
});

app.get('/show/head/:resultid', function(req, res){
  var client = shibclient(req);
  var file = client.generatePath(req.params.resultid);
  if (! fs.existsSync(file)) {
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
    if (line_number < SHOW_RESULT_HEAD_LINES) {
      res.write(line + '\n');
      line_number++;
    } else {
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
  shibclient(req).getQueryByResultId(req.params.resultid, function(err, query){
    if (err) { error_handle(req, res, err); this.end(); return; }

    if (query === null) {
      res.send(null);
      this.end();
      return;
    }

    res.attachment(req.params.resultid + '.tsv');
    res.set('X-Shib-Query-ID', query.queryid);
    res.set('X-Shib-Result-ID', query.resultid);
    res.set('X-Shib-Executed-At', new Date(query.datetime).getTime());
    res.set('X-Shib-Completed-At', query.result.completed_msec || 0);

    var client = shib.client();
    var file = client.generatePath(req.params.resultid);
    if (! fs.existsSync(file)) {
      res.send(null);
      res.end();
      client.end();
      return;
    }
    res.sendfile(file)
  });
});

app.get('/download/csv/:resultid', function(req, res){
  shibclient(req).getQueryByResultId(req.params.resultid, function(err, query){
    if (err) { error_handle(req, res, err); this.end(); return; }

    if (query === null) {
      res.send(null);
      this.end();
      return;
    }

    res.attachment(req.params.resultid + '.csv');
    res.set('X-Shib-Query-ID', query.queryid);
    res.set('X-Shib-Result-ID', query.resultid);
    res.set('X-Shib-Executed-At', new Date(query.datetime).getTime());
    res.set('X-Shib-Completed-At', query.result.completed_msec || 0);

    var client = shib.client();
    var file = client.generatePath(req.params.resultid);
    if (! fs.existsSync(file)) {
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

shib.auth(); // to initialize and check auth module
shib.client().end(); // to initialize sqlite3 database

shib.logger().info('Starting shib.');

if (! shib.auth().require_always){
  var d = require('domain').create();
  d.on('error', function(err){
    // Failed to update engine cache
    // This may occur by communication errors w/ servers
    if (err.code === 'ECONNREFUSED') {
      var e = err.domainEmitter;
      shib.logger().error('Connection refused', {host: e.host, port: e.port});
    } else {
      shib.logger().error('In domain creation', err);
    }
    process.exit();
  });
  d.run(function(){
    var enginesCacheUpdate = function(){
      var AccessControl = require('shib/access_control').AccessControl;
      // generate cache w/ all engines and databases forcely
      //  this cache isn't used for the situation configured as auth:require_always
      shib.client({credential: AccessControl.defaultAllowDelegator()}).engineInfo(function(err, info){
        if (!err && info)
          enginesCache = info;
        this.end();
      });
    };
    setInterval(enginesCacheUpdate, 60*60*1000); // cache update per hour
    enginesCacheUpdate();
  });
}

app.listen(app.get('port'));
