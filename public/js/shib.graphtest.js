function render_google_chart_api(chart_div_id, title, columns, data){
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
  chart.draw(datatable, {width: 800, height: 480, title:title});
};
