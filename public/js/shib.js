google.load("visualization", "1", {packages:["corechart"]});

var shibdata = {};

$(function(){
  $.getJSON('/summary_bulk', function(data){
    shibdata.history = data.history;
    shibdata.keywords = data.keywords;
    shibdata.history_ids = data.history_ids;
    shibdata.keyword_ids = data.keyword_ids;
    shibdata.query_cache = {};
    shibdata.result_cache = {};
    $.ajax({
      url: '/queries',
      type: 'POST',
      dataType: 'json',
      data: {ids: data.query_ids},
      success: function(data){
        var resultids = [];
        data.queries.forEach(function(v){
          shibdata.query_cache[v.queryid] = v;
          if (v.results && v.results.length > 0)
            resultids = resultids.concat(v.results.map(function(r){return r.resultid;}));
        });
        $.ajax({
          url: '/results',
          type: 'POST',
          dataType: 'json',
          data: {ids: resultids},
          success: function(data){
            data.results.forEach(function(r){
              shibdata.result_cache[r.resultid] = r;
            });
            update_history_tab();
            update_keywords_tab();
        
            $("#tab-history").accordion({header:"h3"});
            $("#tab-keywords").accordion({header:"h3"});
            $("#tab-yours").accordion({header:"h3"});
            $("#listSelector").tabs();
          }
        });
      }
    });
  });

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
  $('#test_show_notice_bar').click(function(event){$('#infoarea').toggle();});
  $('#test_show_error_bar').click(function(event){$('#errorarea').toggle();});
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

$.template("queryItemTemplate",
           '<div><div class="queryitem" id="query_${QueryId}">' +
           '  <div class="queryitem_information">${Information}</div>' +
           '  <div class="queryitem_statement">${Statement}</div>' +
           '  <div class="queryitem_status">' +
           '    <span class="status_${Status}">${Status}</span>' +
           '    <span class="queryitem_etc">${Etc}</span>' +
           '  </div>' +
           '</div></div>');

function create_queryitem_object(queryid, id_prefix){
  var query = shibdata.query_cache[queryid];
  if (! query)
    return '';
  var lastresult = (query.results && query.results.length > 0 && query.results[query.results.length - 1]) || null;
  var lastresultobj = (lastresult && shibdata.result_cache[lastresult.resultid]) || null;
  var executed_at = (lastresult && lastresult.executed_at) || '-';
  var keyword_primary = (query.keywords && query.keywords.length > 0 && query.keywords[0]) || '-';
  return {
    QueryId: (id_prefix || '') + query.queryid,
    Information: executed_at + ', ' + keyword_primary,
    Statement: query.querystring,
    Status: (lastresultobj && lastresultobj.status) || 'running',
    Etc: (lastresultobj && lastresultobj.bytes && lastresultobj.lines &&
          (lastresultobj.bytes + ' bytes, ' + lastresultobj.lines + ' lines')) || ''
  };
};

function update_history_tab(){
  $('#tab-history').empty().html(
    shibdata.history.map(function(v){
      var itemsHtml = $.tmpl("queryItemTemplate", shibdata.history_ids[v].map(function(id){
        return create_queryitem_object(id, 'history_');}));
      return '<div><h3><a href="#">' + v + '</a></h3><div>' + itemsHtml.html() + '</div></div>';
    }).join('')
  );
};

function update_keywords_tab(){
  $('#tab-keywords').empty().html(
    shibdata.keywords.map(function(k){
      var itemsHtml = $.tmpl("queryItemTemplate", shibdata.history_ids[v].map(function(id){
        return create_queryitem_object(id, 'keyword_');}));
      return '<div><h3><a href="#">' + v + '</a></h3><div>' + itemsHtml.html() + '</div></div>';
    }).join('')
  );
};

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
