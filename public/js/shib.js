google.load("visualization", "1", {packages:["corechart"]});
$(function(){
  // Tabs + Accordions
  $("#tab-keywords").accordion({header:"h3"});
  $("#tab-history").accordion({header:"h3"});
  $("#ListSelector").tabs();

    // Grapharea
    $("#graph_render_execute_1").click(function(event){
      render_google_chart_api("chart_div", "Daily transferred bytes", shib_test_data1_columns, shib_test_data1);
      return false;
    });
    $("#graph_render_execute_2").click(function(event){
      render_google_chart_api("chart_div", "blog PV data", shib_test_data2_columns, shib_test_data2);
      return false;
    });

    // Accordion
    $("#accordion").accordion({ header: "h3" });

    // Tabs
    $('#tabs').tabs();

    // Dialog			
    $('#dialog').dialog({
      autoOpen: false,
      width: 600,
      buttons: {
        "Ok": function() { 
          $(this).dialog("close"); 
        }, 
        "Cancel": function() { 
          $(this).dialog("close"); 
        } 
      }
    });

    // Dialog Link
    $('#dialog_link').click(function(){
      $('#dialog').dialog('open');
      return false;
    });

    // Datepicker
    $('#datepicker').datepicker({
      inline: true
    });

    // Slider
    $('#slider').slider({
      range: true,
      values: [17, 67]
    });

    // Progressbar
    $("#progressbar").progressbar({
      value: 20 
    });

    //hover states on the static widgets
    $('#dialog_link, ul#icons li').hover(
      function() { $(this).addClass('ui-state-hover'); }, 
      function() { $(this).removeClass('ui-state-hover'); }
    );
});
