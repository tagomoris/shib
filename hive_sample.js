var thrift = require('thrift'),
    ttransport = require('thrift/transport'),
    ThriftHive = require('./gen-nodejs/ThriftHive'),
    ttypes = require('./gen-nodejs/hive_service_types');

var connection = thrift.createConnection("localhost", 10000, {transport: ttransport.TBufferedTransport, timeout: 600*1000}),
    client = thrift.createClient(ThriftHive, connection);

connection.on('error', function(err) {
    console.error(err);
});

connection.addListener("connect", function() {
    client.execute('select count(*) from p', function(err){
        console.error("pos");
        if (err) { console.error("error on execute(): " + err); process.exit(1); }
        
        client.fetchAll(function(err, data){
            if (err){ console.error("error on fetchAll(): " + err); process.exit(1); }
            console.error(data);
            connection.end();
            process.exit(0);
        });
    });
});

