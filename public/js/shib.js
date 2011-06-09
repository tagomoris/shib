google.load("visualization", "1", {packages:["corechart"]});
$(function(){
  $("#tab-history").accordion({header:"h3"});
  $("#tab-keywords").accordion({header:"h3"});
  $("#tab-yours").accordion({header:"h3"});
  $("#listSelector").tabs();

    // Grapharea
  $("#graph_render_execute_1").click(function(event){
    render_google_chart_api("chart_div", "Daily transferred bytes", shib_test_data1_columns, shib_test_data1, {width: 750, height: 450});
    return false;
  });
  $("#graph_render_execute_2").click(function(event){
    render_google_chart_api("chart_div", "blog PV data", shib_test_data2_columns, shib_test_data2, {width: 750, height: 450});
    return false;
  });

  //hover states on the static widgets
  $('ul#icons li, ul#icons2 li').hover(
    function() { $(this).addClass('ui-state-hover'); }, 
    function() { $(this).removeClass('ui-state-hover'); }
  );
});
