var thrift = require('thrift'),
    ttransport = require('thrift/transport'),
    ThriftHive = require('gen-nodejs/ThriftHive');

var connection = thrift.createConnection("localhost", 10000, {transport: ttransport.TBufferedTransport, timeout: 600*1000}),
    client = thrift.createClient(ThriftHive, connection);

connection.on('error', function(err) {
  console.error(err);
});

connection.addListener("connect", function() {
  client.getClusterStatus(function(err, data){
    console.log("getClusterStatus:", data);
    client.execute('select x, count(*) as cnt from p group by x sort by cnt limit 10', function(err){
      if (err) { console.error("error on execute(): " + err); process.exit(1); }

      client.getQueryPlan(function(err, data){
        console.log("getQueryPlan:", data);
        console.log("queryplan queryAttributes:", data.queries[0].queryAttributes);
        console.log("queryplan stageGraph:", data.queries[0].stageGraph);
        console.log("queryplan stageGraph adjacencyList children:", data.queries[0].stageGraph.adjacencyList[0].children);
        console.log("queryplan stageGraph adjacencyList children:", data.queries[0].stageGraph.adjacencyList[1].children);
        console.log("queryplan stageList:", data.queries[0].stageList);
        console.log("queryplan stageList taskList:", data.queries[0].stageList[0].taskList[0]);
        console.log("queryplan stageList taskList operatorGraph adjacencyList:", data.queries[0].stageList[0].taskList[0].operatorGraph.adjacencyList);
        console.log("queryplan stageList taskList:", data.queries[0].stageList[0].taskList[1]);
        console.log("queryplan stageList taskList:", data.queries[0].stageList[1].taskList[0]);
        console.log("queryplan stageList taskList:", data.queries[0].stageList[1].taskList[1]);
        console.log("queryplan stageList taskList:", data.queries[0].stageList[2].taskList[0]);
        console.log("queryplan stageList taskList:", data.queries[0].stageList[2].taskList[1]);

        client.getSchema(function(err, data){
          console.log("getSchema:", data);
          client.getThriftSchema(function(err,data){
            console.log("getThriftSchema:", data);
            client.fetchAll(function(err, data){
              if (err){ console.error("error on fetchAll(): " + err); process.exit(1); }
              console.log("fetchAll:", data);
              connection.end();
              process.exit(0);
            });
          });
        });
      });
    });
  });
});
