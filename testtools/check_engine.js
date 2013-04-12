var engine = require('shib/engine');

// node check_engine.js CONFIG_JS_PATH OPERATION args ...
//
// ex: node check_engine ./config.js execute "SELECT ..."
//     node check_engine ./config.js status JOBID JOBNAME

// argv: ['node', 'script_path', arguments]
var args = process.argv.concat();
args.shift(); args.shift();

var conf_path = args.shift(),
    operation = args.shift();

var conf = JSON.parse(require('fs').readFileSync(conf_path));

var obj = new engine.Engine(conf.executer, conf.monitor);

console.log('engine operation:' + operation + ', args:' + JSON.stringify(args));
if (operation === 'execute') {
  obj.execute('check-engine-query', args[0], {callback: function(err,data){
    if (err) {
      console.log('error:');
      console.log(err);
    }
    console.log('data:');
    console.log(data);
  }});
} else if (operation === 'status') {
  obj.status(args[0], function(err,status){
    console.log({err:err, status:status});
  });
} else if (operation === 'kill') {
  obj.status(args[0], args[1], function(err){
    console.log({err:err});
  });
} else {
  console.log('unknown operation:' + operation);
}
