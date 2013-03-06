var thrift = require('thrift'),
    ttransport = require('thrift/lib/thrift/transport'),
    ThriftHive = require('./ThriftHive');

var Executer = exports.Executer = function(conf){
  if (conf.name !== 'hiveserver')
    throw "executer name mismatch for hiveserver:" + conf.name;

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
  case 'setup':
  case 'execute':
    return true;
  }
  throw "unknown operation name (for hiveserver.Executer):" + operation;
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

Executer.prototype.execute = function(jobname, query, callback){
  var client = this._client;
  //TODO: set jobname: 'set mapreduce.job.name="${jobname}"' if jobname is not null or blank ?

  client.execute(query, function(err){
    if (err) {
      callback(err); return;
    }
    callback(null, new Fetcher(client));
  });
};

var Fetcher = function(client){
  this._client = client;

  this.schema = function(callback){
    this._client.getSchema(function(err, data){
      if (err) { callback(err); return; }
      callback(null, data);
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
