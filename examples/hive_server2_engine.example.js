var engine = require('shib/engines/hiveserver2');
var executer = new engine.Executer({"name":"hiveserver2", "host":"hiveserver2.server.local", "port":10001});

executer.execute(
    'jobname',
    'select service, count(*) as cnt, false, 0.01, 1, array(1,2,3) as a from access_log where (service="blog" or service="news") and yyyymmdd="20121122" group by service',
    function(err, fetcher){
      console.log({op:'execute', err:err, fetcher:fetcher});
      if (err) return;
      fetcher.schema(function(err,schema){
        if (err) {
          console.log(JSON.stringify(err, null, "  "));
          return;
        }
        console.log(JSON.stringify(schema, null, "  "));
        fetcher.fetch(null, function(err, rows){
          console.log({op:'fetch', err:err, rows:rows});
          console.log(JSON.stringify(rows, null, "  "));
        });
      });
    }
);

executer.execute(
    null,
    'show tables',
    function(err, fetcher){
      console.log({op:'execute', err:err, fetcher:fetcher});
      if (err) {
        console.log(JSON.stringify(err, null, "  "));
        return;
      }
      fetcher.schema(function(err,res){
        if (err) {
          console.log(JSON.stringify(err, null, "  "));
          return;
        }
        console.log(JSON.stringify(res, null, "  "));
        fetcher.fetch(null, function(err, rows){
          console.log({op:'fetch', err:err, rows:rows});
          console.log(JSON.stringify(rows, null, "  "));
        });
      });
    }
);

executer.execute(
    null,
    'show tables',
    function(err, fetcher){
      console.log({op:'execute', err:err, fetcher:fetcher});
      fetcher.fetch(null, function(err, rows){
        console.log({op:'fetch', err:err, rows:rows});
      });
    }
);

executer.execute(
    null,
    'show tables',
    function(err, fetcher){
      console.log({op:'execute', err:err, fetcher:fetcher});
      fetcher.schema(function(err, schema){
        console.log({op:'schema', err:err, schema:schema});
        fetcher.fetch(null, function(err, rows){
          console.log({op:'fetch', err:err, rows:rows});
        });
      });
    }
);
