var SimpleCSVBuilder = exports.SimpleCSVBuilder = function(args){
};

SimpleCSVBuilder.escape = function(value){
  return String(value).replace(/"/g, '""');
};

SimpleCSVBuilder.build = function(data){
  if (data.length < 1)
    return '';
  var values = [];
  for(var i = 0; i < data.length; i++) {
    var d = data[i];
    if (d === undefined || d === null)
      d = '';
    values.push('"' + SimpleCSVBuilder.escape(d) + '"');
  }
  return values.join(',') + '\n';
};
