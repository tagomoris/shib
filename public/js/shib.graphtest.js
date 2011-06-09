function render_google_chart_api(chart_div_id, title, columns, data, options){
  var default_options = {width: 750, height: 450};
  var datatable = new google.visualization.DataTable();
  var types = [];
  columns.forEach(function(c){
    datatable.addColumn(c.type, c.name);
    types.push(c.type);
  });
  datatable.addRows(data.length);
  for(var i = 0; i < data.length; i++){
    for(var j = 0; j < columns.length; j++){
      if (types[j] == 'number')
        datatable.setValue(i, j, Number(data[i][j]));
      else
        datatable.setValue(i, j, data[i][j]);
    }
  }

  var chart = new google.visualization.LineChart(document.getElementById(chart_div_id));
  if (! options)
    options = {};
  options.title = title;
  if (! options.width)
    options.width = default_options.width;
  if (! options.height)
    options.height = default_options.height;
  chart.draw(datatable, options);
};
