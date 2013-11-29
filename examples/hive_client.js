var thrift = require('../node_modules/thrift'),
    ttransport = require('../node_modules/thrift/lib/thrift/transport'),
    ThriftHive = require('../lib/shib/engines/hiveserver/ThriftHive');

var connection = thrift.createConnection("ec2-54-249-137-203.ap-northeast-1.compute.amazonaws.com", 10004, {transport: ttransport.TBufferedTransport, timeout: 6*1000}),
    client = thrift.createClient(ThriftHive, connection);

connection.on('error', function(err) {
    console.error(err);
});

connection.addListener("connect", function() {
    console.log("connected");
    client.execute('select count(*) from nicodata.videoinfo', function(err){
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

