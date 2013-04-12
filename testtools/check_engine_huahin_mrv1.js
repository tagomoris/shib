var huahin_mrv1 = require('shib/engines/huahin_mrv1');

// node check_engine_huahin_mrv1.js HUAHIN_HOST HUAHIN_PORT OPERATION ARGS
//
// ex: node check_engine_huahin_mrv1.js localhost 9010 status jobname-foo1
//     node check_engine_huahin_mrv1.js localhost 9010 kill jobname-foo1


// argv: ['node', 'script_path', arguments]
var monitor = new huahin_mrv1.Monitor({
  name: 'huahin_mrv1',
  host: process.argv[2],
  port: parseInt(process.argv[3])
});

var operation = process.argv[4],
    jobname = process.argv[5];

console.log('huahin_mrv1 ' + operation + ':' + jobname);
var shutdown = function(){ monitor.close(); };

if (operation === 'status') {
  monitor.status(jobname, function(err,status){
    console.log({err:err, status:status});
    shutdown();
  });
} else if (operation === 'kill') {
  monitor.kill(null, jobname, function(err){
    console.log({err:err});
    shutdown();
  });
} else {
  console.log('unknown operation:' + operation);
  shutdown();
}
