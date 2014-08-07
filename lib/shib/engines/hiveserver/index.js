var thrift = require('node-thrift'),
    ttransport = require('node-thrift/lib/thrift/transport'),
    ThriftHive = require('./ThriftHive');

var Executer = exports.Executer = function(conf, logger){
  if (conf.name !== 'hiveserver')
    throw "executer name mismatch for hiveserver:" + conf.name;

  this.logger = logger;

  this._connection = thrift.createConnection(
    conf.host,
    conf.port,
    {transport: ttransport.TBufferedTransport}
  );
  this._client = thrift.createClient(ThriftHive, this._connection);
};

Executer.prototype.end = function(){
  if (this._client) {
    var self = this;
    this._client.clean(function(){
        self._connection.end();
    });
  }
};

Executer.prototype.supports = function(operation){
  switch (operation) {
  case 'jobname':
  case 'setup':
  case 'databases':
  case 'tables':
  case 'partitions':
  case 'describe':
  case 'execute':
    return true;
  }
  throw "unknown operation name (for hiveserver.Executer):" + operation;
};

Executer.prototype.jobname = function(queryid){
  return 'shib-hs1-' + queryid;
};

Executer.prototype.setup = function(setups, callback){
  if (!setups || setups.length < 1) {
    callback(null); return;
  }

  var client = this._client;
  var setupQueue = setups.concat(); // shallow copy of Array to use as queue
  var executeSetup = function(queue, callback){
    var q = queue.shift();
    client.execute(q, function(err){
      if (err)
        callback(err);
      else
        client.fetchAll(function(err, data){
          if (queue.length > 0)
            executeSetup(queue, callback);
          else
            callback(null);
        });
    });
  };
  executeSetup(setupQueue, callback);
};

Executer.prototype.databases = function(callback){
  var client = this._client;
  client.execute('show databases', function(err){
    if (err)
      callback(err);
    else
      client.fetchAll(function(err, data){
        if (err) { callback(err); return; }
        callback(null, data);
      });
  });
};

Executer.prototype.tables = function(dbname, callback){
  var client = this._client;
  this.setup(['use ' + dbname], function(err){
    if (err) { callback(err); return; }
    client.execute('show tables', function(err){
      if (err)
        callback(err);
      else
        client.fetchAll(function(err, data){
          if (err) { callback(err); return; }
          callback(null, data);
        });
    });
  });
};

Executer.prototype.partitions = function(dbname, tablename, callback){
  var client = this._client;
  this.setup(['use ' + dbname], function(err){
    if (err) { callback(err); return; }
    client.execute('show partitions ' + tablename, function(err){
      if (err) {
        callback(err);
        return;
      }
      client.fetchAll(function(err, data){
        callback(err, data);
      });
    });
  });
};

Executer.prototype.describe = function(dbname, tablename, callback){
  var client = this._client;
  this.setup(['use ' + dbname], function(err){
    if (err) { callback(err); return; }
    client.execute('describe ' + tablename, function(err){
      if (err)
        callback(err);
      else
        client.fetchAll(function(err, data){
          if (err) { callback(err); return; }
          var rows = data.map(function(row){ return row.split('\t'); });
          callback(null, rows);
        });
    });
  });
};

Executer.prototype.execute = function(jobname, dbname, query, callback){
  var client = this._client;

  var settings = [];
  if (dbname)
    settings.push('use ' + dbname);
  if (jobname && jobname !== '') {
    settings.push('set mapred.job.name=' + jobname);
    settings.push('set mapreduce.job.name=' + jobname);
  }
  this.setup(settings, function(err){
    if (err) { callback(err); return; }

    client.execute(query, function(err){
      if (err) {
        callback(err); return;
      }
      callback(null, new Fetcher(client));
    });
  });
};

var Fetcher = function(client){
  this._client = client;

  this.schema = function(callback){
    this._client.getSchema(function(err, data){
      if (err) { callback(err); return; }
      callback(null, data.fieldSchemas);
    });
  };

  this.fetch = function(num, callback){
    if (!num) {
      this._client.fetchAll(callback);
    } else {
      this._client.fetchN(num, callback);
    }
  };
};

// has no Monitor
