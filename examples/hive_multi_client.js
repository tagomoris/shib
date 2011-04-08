var thrift = require('thrift'),
    ttransport = require('thrift/transport'),
    ThriftHive = require('gen-nodejs/ThriftHive');

var connection1 = thrift.createConnection("localhost", 10000, {transport: ttransport.TBufferedTransport, timeout: 1*1000}),
    client1 = thrift.createClient(ThriftHive, connection1);
var connection2 = thrift.createConnection("localhost", 10000, {transport: ttransport.TBufferedTransport, timeout: 1*1000}),
    client2 = thrift.createClient(ThriftHive, connection2);

connection1.on('error', function(err){ console.error(err); });
connection2.on('error', function(err){ console.error(err); });

var query1 = 'select x, count(*) from p group by x';
var query2 = 'select x, count(*) as cnt, "hoge" from p group by x sort by cnt desc limit 30';
var done1 = false;
var done2 = false;

var run_test = function(conn, client, query, label, wait, callback) {
  var func = function(){
    console.log(label, "executing.");
    client.execute(query, function(err){
      console.log(label, "executed.");
      if (err){ console.error(label, "error on execute():", err); }
      
      client.fetchAll(function(err, data){
        console.log(label, "fetched.");
        if (err){ console.error(label, "error on fetchAll():", err); }

        console.log(label, "result:", data);
        callback();
      });
    });
  };
  conn.addListener("connect", function(){
    console.log(label, "connected.");
    setTimeout(func, wait);
  });
};

run_test(connection1, client1, query1, "conn1:", 5000, function(){ done1 = true; });
run_test(connection2, client2, query2, "conn2:", 1000, function(){ done2 = true; });

setInterval(function(){
  if (done1 && done2){
    connection1.end();
    connection2.end();
    process.exit(0);
  }
}, 1000);
