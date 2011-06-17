google.load("visualization", "1", {packages:["corechart"]});

var shibnotifications = [];
var shibdata = {};
var shibselectedquery = null;
var shibselectedquery_dom = null;

var shib_NOTIFICATION_CHECK_INTERVAL = 100;
var shib_NOTIFICATION_LAST_DURATION_COUNT = 200;

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
        data.queries.forEach(function(query1){
          shibdata.query_cache[query1.queryid] = query1;
          if (query1.results && query1.results.length > 0)
            resultids = resultids.concat(query1.results.map(function(r){return r.resultid;}));
        });
        $.ajax({
          url: '/results',
          type: 'POST',
          dataType: 'json',
          data: {ids: resultids},
          success: function(data){
            data.results.forEach(function(result1){
              shibdata.result_cache[result1.resultid] = result1;
            });
            update_history_tab();
            update_keywords_tab();
        
            $("#tab-history").accordion({header:"h3"});
            $("#tab-keywords").accordion({header:"h3"});
            $("#tab-yours").accordion({header:"h3"});
            $("#listSelector").tabs();

            $('.queryitem').click(select_queryitem);

            setInterval(show_notification, shib_NOTIFICATION_CHECK_INTERVAL);
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

var shib_current_notification = null;
var shib_current_notification_counter = 0;
function show_notification(){
  if (shib_current_notification === null && shibnotifications.length == 0)
    return;
  if (shib_current_notification !== null && shibnotifications.length == 0){
    shib_current_notification_counter += 1;
    if (shib_current_notification_counter > shib_NOTIFICATION_LAST_DURATION_COUNT) {
      shib_current_notification.fadeOut(100);
      shib_current_notification_counter = 0;
    }
    return; 
  }
  var next = shibnotifications.shift();
  shib_current_notification_counter = 0;
  if (shib_current_notification) {
    shib_current_notification.fadeOut(100, function(){
      shib_current_notification = update_notification(next.type, next.title, next.message);
      shib_current_notification.fadeIn(100);
    });
  }
  else {
    shib_current_notification = update_notification(next.type, next.title, next.message);
    shib_current_notification.fadeIn(100);
  }
};

function update_notification(type, title, message){
  if (type === 'info') {
    $('#infotitle').text(title);
    $('#infomessage').text(message);
    return $('#infoarea');
  }
  $('#errortitle').text(title);
  $('#errormessage').text(message);
  return $('#errorarea');
};

function show_info(title, message){
  shibnotifications.push({type:'info', title:title, message:message});
};

function show_error(title, message){
  shibnotifications.push({type:'error', title:title, message:message});
};


$.template("queryItemTemplate",
           '<div><div class="queryitem" id="query-${QueryId}">' +
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
    Status: (lastresultobj && lastresultobj.state) || 'running',
    Etc: (lastresultobj && lastresultobj.bytes && lastresultobj.lines &&
          (lastresultobj.bytes + ' bytes, ' + lastresultobj.lines + ' lines')) || ''
  };
};

function update_history_tab(){
  var history_num = 1;
  $('#tab-history').empty();
  shibdata.history.forEach(function(history1){
    var historyitemlistid = 'history-idlist-' + history_num;
    $('#tab-history').append('<div><h3><a href="#">' + history1 + '</a></h3><div id="' + historyitemlistid + '"></div></div>');
    $.tmpl("queryItemTemplate",
           shibdata.history_ids[history1].map(function(id){
             return create_queryitem_object(id, 'history-');})
          ).appendTo('#tab-history div div#' + historyitemlistid);
    history_num += 1;
  });
};

function update_keywords_tab(){
  var keyword_num = 1;
  $('#tab-keywords').empty();
  shibdata.keywords.forEach(function(keyword1){
    var keyworditemlistid = 'keyword-idlist-' + keyword_num;
    $('#tab-keywords').append('<div><h3><a href="#">' + keyword1 + '</a></h3><div id="' + keyworditemlistid + '"></div></div>');
    $.tmpl("queryItemTemplate",
           shibdata.keyword_ids[keyword1].map(function(id){
             return create_queryitem_object(id, 'keyword-');})
          ).appendTo('#tab-keywords div div#' + keyworditemlistid);
    keyword_num += 1;
  });
};

function set_selected_query(query, dom){
  release_selected_query();
  $(dom).addClass('queryitem_selected');
  shibselectedquery_dom = dom;
  shibselectedquery = query;
};

function release_selected_query(){
  if (! shibselectedquery)
    return;
  $(shibselectedquery_dom).removeClass('queryitem_selected');
  shibselectedquery_dom = null;
  shibselectedquery = null;
};

function select_queryitem(event){
  var target_dom = $(event.target).closest('.queryitem');
  var target_dom_id = target_dom.attr('id');
  var dom_id_regex = /^query-(keyword|history)-([0-9a-f]+)$/;
  var match_result = dom_id_regex.exec(target_dom_id);
  if (match_result === null) {
    show_error("UI Bug", "Selected DOM id invalid:" + target_dom_id);
    return;
  }
  var query = shibdata.query_cache[match_result[2]];
  if (! query) {
    show_error("UI Bug", "Selected query not loaded on browser:" + match_result[2]);
    return;
  }
  
  set_selected_query(query, target_dom);
  update_mainview(query);
};

function update_mainview(query){
  shibselectedquery = query;
  $('#queryeditor').text(query.querystring);
  /*TODO: write!!! */
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
