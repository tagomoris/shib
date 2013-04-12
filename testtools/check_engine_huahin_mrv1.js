// node check_engine_huahin_mrv1.js HUAHIN_HOST HUAHIN_PORT OPERATION ARGS
//
// ex: node check_engine_huahin_mrv1.js localhost 9010 status jobname-foo1
//     node check_engine_huahin_mrv1.js localhost 9010 kill jobname-foo1

var huahin_mrv1 = require('shib/engines/huahin_mrv1');

var monitor = new huahin_mrv1.Monitor({
  name: 'huahin_mrv1',
  host: process.argv[0],
  port: parseInt(process.argv[1])
});

var operation = process.argv[2],
    jobname = process.argv[3];

console.log('huahin_mrv1 ' + operation + ':' + jobname);
if (operation === 'status') {
  monitor.status(jobname, function(err,status){
    console.log({err:err, status:status});
  });
} else if (operation === 'kill') {
  monitor.kill(null, jobname, function(err){
    console.log({err:err});
  });
} else {
  console.log('unknown operation:' + operation);
}