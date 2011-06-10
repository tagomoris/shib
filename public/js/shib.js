google.load("visualization", "1", {packages:["corechart"]});

$(function(){
  $("#tab-history").accordion({header:"h3"});
  $("#tab-keywords").accordion({header:"h3"});
  $("#tab-yours").accordion({header:"h3"});
  $("#listSelector").tabs();

  //hover states on the static widgets
  $('ul.operationitems li').hover(
    function() { $(this).addClass('ui-state-hover'); }, 
    function() { $(this).removeClass('ui-state-hover'); }
  );


  /* **** effects, and events for tests **** */
  $('ul#icons1 li, ul#icons2 li').hover(
    function() { $(this).addClass('ui-state-hover'); }, 
    function() { $(this).removeClass('ui-state-hover'); }
  );
  $('#test_status_change_not_executed').click(function(event){shib_test_status_change("not executed");});
  $('#test_status_change_running').click(function(event){shib_test_status_change("running");});
  $('#test_status_change_executed').click(function(event){shib_test_status_change("executed");});
  $('#test_status_change_error').click(function(event){shib_test_status_change("error");});
  $('#test_status_change_rerunning').click(function(event){shib_test_status_change("rerunning");});
  /* **** **** */

  // Grapharea
  $("#graph_render_execute_1").click(function(event){
    render_google_chart_api("chart_div", "Daily transferred bytes", shib_test_data1_columns, shib_test_data1, {width: 650, height: 450});
    return false;
  });
  $("#graph_render_execute_2").click(function(event){
    render_google_chart_api("chart_div", "blog PV data", shib_test_data2_columns, shib_test_data2, {width: 650, height: 450});
    return false;
  });
});

function shib_test_status_change(next_state) {
  if (next_state == 'not executed'){
    $('li#execute_button').show(); $('li#pause_button').hide(); $('li#rerun_button').hide(); $('li#display_full_button').hide();
    $('li#display_head_button').hide(); $('li#download_tsv_button').hide(); $('li#download_csv_button').hide();
    $('span#querystatus')
      .removeClass('status_running status_executed status_error status_rerunning')
      .addClass('status_not_executed')
      .text('not executed');
    $('span#queryresult').hide();
  }
  else if (next_state == 'running'){
    $('li#execute_button').hide(); $('li#pause_button').show(); $('li#rerun_button').hide(); $('li#display_full_button').hide();
    $('li#display_head_button').hide(); $('li#download_tsv_button').hide(); $('li#download_csv_button').hide();
    $('span#querystatus')
      .removeClass('status_not_executed status_executed status_error status_rerunning')
      .addClass('status_running')
      .text('running');
    $('span#queryresult').hide();
  }
  else if (next_state == 'executed'){
    $('li#execute_button').hide(); $('li#pause_button').hide(); $('li#rerun_button').show(); $('li#display_full_button').show();
    $('li#display_head_button').show(); $('li#download_tsv_button').show(); $('li#download_csv_button').show();
    $('span#querystatus')
      .removeClass('status_not_executed status_running status_error status_rerunning')
      .addClass('status_executed')
      .text('executed');
    $('span#queryresult').show();
    $('span#queryresultlines').text(" 512 lines, ");
    $('span#queryresultbytes').text(" 2049 bytes");
  }
  else if (next_state == 'error'){
    $('li#execute_button').hide(); $('li#pause_button').hide(); $('li#rerun_button').show(); $('li#display_full_button').hide();
    $('li#display_head_button').hide(); $('li#download_tsv_button').hide(); $('li#download_csv_button').hide();
    $('span#querystatus')
      .removeClass('status_not_executed status_running status_executed status_rerunning')
      .addClass('status_error')
      .text('error');
    $('span#queryresult').hide();
  }
  else if (next_state == 'rerunning'){
    $('li#execute_button').hide(); $('li#pause_button').show(); $('li#rerun_button').hide(); $('li#display_full_button').show();
    $('li#display_head_button').show(); $('li#download_tsv_button').show(); $('li#download_csv_button').show();
    $('span#querystatus')
      .removeClass('status_not_executed status_running status_executed status_error')
      .addClass('status_rerunning')
      .text('re-running');
    $('span#queryresult').show();
    $('span#queryresultlines').text(" 512 lines, ");
    $('span#queryresultbytes').text(" 2049 bytes");
  }
}
