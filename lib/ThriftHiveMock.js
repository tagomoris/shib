var hive_service_types = require('gen-nodejs/hive_service_types'),
    hive_metastore_types = require('gen-nodejs/hive_metastore_types'),
    queryplan_types = require('gen-nodejs/queryplan_types');

var chars = [
  'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
  'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
  '-', ' ', '_', '\'', '"', '?', '!', '=', '+', '/', '.', ','
];
var namechars = [
  'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
  'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
  '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'
];
var alphabet_namechars = [
  'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
  'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
];
var random_num = function(max){ return Math.floor(Math.random() * max) + 1; };
var random_index = function(max){ return Math.floor(Math.random() * max); };
var choose = function(list){
  return list[random_index(list.length)];
};
var random_string = function(len){
  var ret = '';
  for (var i = 0; i < len; i++){ ret += choose(chars); }
  return ret;
};
var random_name = function(len){
  var ret = '';
  for (var i = 0; i < len; i++){ ret += choose(namechars); }
  return ret;
};
var random_alphabetname = function(len){
  var ret = '';
  for (var i = 0; i < len; i++){ ret += choose(alphabet_namechars); }
  return ret;
};
exports.cluster_status = function(){
  return new hive_service_types.HiveClusterStatus({
    taskTrackers: 1,
    mapTasks: 0,
    reduceTasks: 0,
    maxMapTasks: 2,
    maxReduceTasks: 2,
    state: 2
  });
};

var idlist = ['hadoop_20110408154949_8b2be199-02ae-40fe-9492-d197ade572f2',
              'hadoop_20110408154949_8b2be199-03ff-40fe-9492-d197ade572f2',
              'hadoop_20110408154949_8b2be199-04bc-40fe-9492-d197ade572f2',
              'hadoop_20110408154949_8b2be199-05db-40fe-9492-d197ade572f2',
              'hadoop_20110408154949_8b2be199-06fc-40fe-9492-d197ade572f2'];
var queryId = function(){ return choose(idlist); };
var operator = function(num){
  var o = new queryplan_types.Operator({
    operatorId: 'TS_12' + num,
    operatorType: Math.floor(Math.random() * 10),
    operatorAttributes: null,
    operatorCounters: null,
    done: true,
    started: true
  }); 
  return o;
};
var operatorGraph = function(ops){
  var al = [];
  for (var i = 0; i < ops.length - 1; i++){
    al.push(new queryplan_types.Adjacency({ node: ops[i].operatorId, children: [ ops[i+1].operatorId ], adjacencyType: 0 }));
  }
  return new queryplan_types.Graph({
    nodeType: 0,
    roots: null,
    adjacencyList: al
  });
};
var task = function(stage,mapreduce,operators){
  var ops = [];
  for (var i = 0; i < operators; i++){ ops.push(operator(i)); }
  return new queryplan_types.Task({
    taskId: 'Stage-' + stage + '_' + mapreduce,
    taskType: (mapreduce == 'MAP' ? 0 : 1),
    taskAttributes: null,
    taskCounters: null,
    operatorList: ops,
    operatorGraph: operatorGraph(ops),
    done: true,
    started: true
  });
};

var stage = function(stage){
  var cntr_map = 'CNTR_NAME_Stage-' + stage + '_MAP_PROGRESS';
  var cntr_reduce = 'CNTR_NAME_Stage-' + stage + '_REDUCE_PROGRESS';
  var counters = {};
  counters[cntr_map] = 100;
  counters[cntr_reduce] = 100;
  return new queryplan_types.Stage({
    stageId: 'Stage-' + stage,
    stageType: 3,
    stageAttributes: null,
    stageCounters: counters,
    taskList: [task(stage,'MAP',3), task(stage,'REDUCE',1)],
    done: true,
    started: true
  });
};

exports.query_plan = function(querystring){
  if (querystring == undefined){
    return new queryplan_types.QueryPlan({});
  }

  var query = new queryplan_types.Query({
    queryId: queryId(),
    queryType: null,
    queryAttributes: { queryString: querystring },
    queryCounters: null,
    stageList: [stage(1), stage(2)],
    stageGraph: new queryplan_types.Graph({
      nodeType: 1,
      roots: null,
      adjacencyList: [ new queryplan_types.Adjacency({ node: 'Stage-1', children: [ 'Stage-2' ], adjacencyType: 0 }) ]
    }),
    done: true,
    started: true
  });
  return new queryplan_types.QueryPlan({ queries: [query], done: false, started: false });
};

var columns = function(query){
  var match = /select (.*) from .*/im.exec(query);
  if (! match)
    throw new Error('query field definition invalid!');
  return match[1].split(/, /).map(function(s){return s.trim();});
};

var columninfo = function(column){
  var name = column;
  var type = 'string';
  var ex = undefined;

  var match = /as ([_a-zA-Z0-9]*)$/im.exec(column);
  if (match){
    name = match[1];
  }
  if (/^count/im.exec(column)) {
    type = 'bigint';
    ex = 'count';
  }
  else if (/^(sum|avg|min|max)/im.exec(column)) {
    type = 'bigint';
    ex = 'aggr';
  }
  else if (/id$/.exec(name)) {
    type = 'bigint';
    ex = 'id';
  }

  if (/^"(.*)"$/.exec(name)) {
    name = /^"(.*)"$/.exec(name)[1];
    ex = "strcopy";
  }
  else if (name == 'yyyymmdd') {
    ex = 'date';
  }
  else if (name == 'hhmm' || name == 'hhmmss') {
    ex = 'time';
  }
  else if (/name$/i.exec(name)) {
    ex = 'name';
  }

  return {name: name, type: type, ex: ex};
};

exports.schema = function(query){
  if (! query) {
    return new hive_metastore_types.Schema({});
  }

  var cols = columns(query.split('\n').join(' '));
  return new hive_metastore_types.Schema({
    fieldSchemas: cols.map(function(c){
      var i = columninfo(c);
      return new hive_metastore_types.FieldSchema({name: i.name, type: i.type, comment: undefined});
    }),
    properties: null
  });
};

var generateValue = function(colinfo){
  function pad(n){return n<10 ? '0'+n : n;}
  switch(colinfo.ex) {
  case 'strcopy':
    return colinfo.name;
  case 'date':
    var d1 = new Date((new Date()).getTime() - random_num(50) * 86400 * 1000);
    return '' + d1.getFullYear() + pad(d1.getMonth()+1) + pad(d1.getDate());
  case 'time':
    var d2 = new Date((new Date()).getTime() - random_num(12 * 60) * 60 * 1000);
    return '' + pad(d2.getHours()) + pad(d2.getMinutes());
  case 'id':
    return random_num(500);
  case 'aggr':
    return random_num(10000);
  case 'count':
    return random_num(2000);
  case 'name':
    return random_name(random_num(10));
  }
  if (colinfo.type == 'string'){
    return random_string(random_num(50));
  }
  return random_num(100);
};

var generate_tablename = exports.generate_tablename = function(){
  var part_depth = choose([1,1,2,2,3,4]);
  var name = '';
  for (var i = 0; i < part_depth; i++) {
    if (name.length > 0)
      name += '_';
    name += random_alphabetname(3) + random_num(3);
  }
  return name;
};

var generate_subtree = exports.generate_subtree = function(subtree_label, parent) {
  var parent_part = parent ? parent + '/' : '';
  var current_depth_label = subtree_label;
  var children_label = null;
  if (subtree_label.indexOf('_') > -1) {
    var separator = subtree_label.indexOf('_');
    current_depth_label = subtree_label.substring(0, separator);
    children_label = subtree_label.substring(separator + 1);
  }
  
  var matched = /^([a-z]+)(\d+)$/.exec(current_depth_label);
  var fieldname = matched[1];
  var partsNum = Number(matched[2]);
  var parts = [];
  for (var i = 0; i < partsNum; i++) {
    var current_part = parent_part + fieldname + '=' + i;
    if (children_label) {
      parts = parts.concat(generate_subtree(children_label, current_part));
    }
    else {
      parts.push(current_part);
    }
  }
  return parts;
};

exports.result = function(query){
  var rows = choose([0,1,1,1,1,2,3,5,7,10,20,50]);
  var matched = null;
  if ((matched = /^show (tables|partitions)( (.*))?$/i.exec(query)) != null) {
    if (/^tables$/i.exec(matched[1])) { /* show tables */
      if (rows < 1)
        rows = 1;
      var tables = [];
      for (var i = 0; i < rows; i++) {
        var name = generate_tablename();
        while (tables.indexOf(name) > -1)
          name = generate_tablename();
        tables.push(name);
      }
      return tables;
    }
    else { /* show partitions hogetable */
      var tablename = matched[3];
      if (! tablename)
        return [];
      return generate_subtree(tablename);
    }
  }
  else {
    var colinfos = columns(query).map(function(c){return columninfo(c);});
    var limitmatch = /limit (\d+)/i.exec(query);
    if (colinfos.length == 1 && colinfos[0].ex == 'count') {
      rows = 1;
    }
    else if (limitmatch) {
      rows = limitmatch[1];
    }
    var values = [];
    for (var i = 0; i < rows; i++){
      values.push(colinfos.map(function(i){ return generateValue(i); }).join("\t"));
    }
    return values;
  }
};
