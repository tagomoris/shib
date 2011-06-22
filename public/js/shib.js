google.load("visualization", "1", {packages:["corechart"]});

var shibnotifications = [];
var shibdata = {};
var shibselectedquery = null;
var shibselectedquery_dom = null;

var shib_QUERY_STATUS_CHECK_INTERVAL = 20000;
var shib_NOTIFICATION_CHECK_INTERVAL = 100;
var shib_NOTIFICATION_DEFAULT_DURATION_SECONDS = 20;

$(function(){
  $.getJSON('/summary_bulk', function(data){
    shibdata.history = data.history;
    shibdata.keywords = data.keywords;
    shibdata.history_ids = data.history_ids;
    shibdata.keyword_ids = data.keyword_ids;
    shibdata.query_cache = {};
    shibdata.query_status_cache = {};
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

            setInterval(check_running_query_status, shib_QUERY_STATUS_CHECK_INTERVAL);
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
  $('#test_status_change_rerunning').click(function(event){shib_test_status_change("re-running");});
  $('#test_show_notice_bar').click(function(event){$('#infoarea').toggle();});
  $('#test_show_error_bar').click(function(event){$('#errorarea').toggle();});
  /* **** **** */

  // mainview controll box (textarea / status / switch buttons) updator
  $('#queryeditor').keypress(function(event){
    if (shibselectedquery && shibselectedquery.querystring !== $(event.target).text())
      deselect_and_new_query();
  });
  $('#queryeditor').change(function(event){
    if (shibselectedquery && shibselectedquery.querystring !== $(event.target).text())
      deselect_and_new_query();
  });

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

/* basic data operations */

function query_last_result(query) {
  var obj = null;
  if (query && query.results && query.results.length > 0 && query.results[query.results.length - 1])
    if ((obj = shibdata.result_cache[query.results[query.results.length - 1].resultid]) !== null)
      return obj;
  return null;
}

function query_current_status(query) {
  if (! (query && query.queryid))
    show_error('UI Bug', 'query id unknown', 5, query);

  if (shibdata.query_status_cache[query.queryid])
    return shibdata.query_status_cache[query.queryid];
  var lastresult = query_last_result(query);
  shibdata.query_status_cache[query.queryid] = (lastresult && lastresult.state) || 'not executed';
  return shibdata.query_status_cache[query.queryid];
};

/* notifications */

var shib_current_notification = null;
var shib_current_notification_counter = 0;
function show_notification(event){ /* event object is not used */
  if (shib_current_notification === null && shibnotifications.length == 0)
    return;
  if (shib_current_notification !== null && shibnotifications.length == 0){
    shib_current_notification_counter -= 1;
    if (shib_current_notification_counter < 1) {
      shib_current_notification.fadeOut(100);
      shib_current_notification_counter = 0;
    }
    return; 
  }
  var next = shibnotifications.shift();
  shib_current_notification_counter = next.duration || shib_NOTIFICATION_DEFAULT_DURATION_SECONDS * 10;
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

function show_info(title, message, duration){
  shibnotifications.push({type:'info', title:title, message:message, duration:duration});
};

function show_error(title, message, duration, optional_object){
  shibnotifications.push({type:'error', title:title, message:message, duration:duration});
  if (optional_object)
    console.log(optional_object);
};

/* right pane operations */

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
  var lastresult = query_last_result(query);
  var executed_at = (lastresult && lastresult.executed_at) || '-';
  var keyword_primary = (query.keywords && query.keywords.length > 0 && query.keywords[0]) || '-';
  return {
    QueryId: (id_prefix || '') + query.queryid,
    Information: executed_at + ', ' + keyword_primary,
    Statement: query.querystring,
    Status: (lastresult && lastresult.state) || 'running',
    Etc: (lastresult && lastresult.bytes && lastresult.lines &&
          (lastresult.bytes + ' bytes, ' + lastresult.lines + ' lines')) || ''
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

function deselect_and_new_query(){
  release_selected_query();
  update_editbox(null);
  show_info('', 'selected query released', 5);
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
    show_error("UI Bug", "Selected DOM id invalid:" + target_dom_id, 5);
    return;
  }
  var query = shibdata.query_cache[match_result[2]];
  if (! query) {
    show_error("UI Bug", "Selected query not loaded on browser:" + match_result[2], 5);
    return;
  }
  
  set_selected_query(query, target_dom);
  update_mainview(query);
};

/* left pane view updates */

function update_mainview(query){
  shibselectedquery = query;
  $('#queryeditor').val(query.querystring);
  update_editbox(query);
};

function update_editbox(query, optional_state) {
  var lastresult = query_last_result(query);
  var state = optional_state || (lastresult && lastresult.state) || null;

  switch (state) {
  case undefined:
  case null:
    // 'not executed'
    show_editbox_buttons(['execute_button']);
    change_editbox_querystatus_style('not executed');
    break;
  case 'running':
    show_editbox_buttons(['pause_button']);
    change_editbox_querystatus_style('running');
    break;
  case 'executed':
  case 'done':
    show_editbox_buttons(['rerun_button', 'display_full_button', 'display_head_button', 'download_tsv_button', 'download_csv_button']);
    change_editbox_querystatus_style('executed', lastresult);
    break;
  case 'error':
    show_editbox_buttons(['rerun_button']);
    change_editbox_querystatus_style('error', lastresult);
    break;
  case 're-running':
    show_editbox_buttons(['pause_button', 'display_full_button', 'display_head_button', 'download_tsv_button', 'download_csv_button']);
    change_editbox_querystatus_style('re-running', lastresult);
    break;
  default:
    show_error('UI Bug', 'unknown query status:' + query.state, 5);
  }
}

function show_editbox_buttons(buttons){
  var allbuttons = [
    'execute_button', 'pause_button', 'rerun_button', 'display_full_button',
    'display_head_button', 'download_tsv_button', 'download_csv_button'
  ];
  if (! buttons)
    buttons = [];
  allbuttons.forEach(function(b){
    if (buttons.indexOf(b) > -1)
      $('li#' + b).show();
    else
      $('li#' + b).hide();
  });
}

function change_editbox_querystatus_style(state, result){
  var allstates = {
    'not executed':{classname:'status_not_executed', result:false},
    'running':{classname:'status_running', result:false},
    'executed':{classname:'status_executed', result:true},
    'error':{classname:'status_error', result:true},
    're-running':{classname:'status_rerunning', result:true}
  };
  var allclasses = 'status_not_executed status_running status_executed status_error status_rerunning';
  if (state === 'done')
    state = 'executed';

  if (allstates[state]) {
    $('span#querystatus')
      .removeClass(allclasses)
      .addClass((allstates[state]).classname)
      .text(state);

    if ((allstates[state]).result) {
      $('span#queryresult').show();
      if (result.error) {
        $('span#queryresultlines').text(result.error);
        $('span#queryresultbytes').text("");
      }
      else {
        $('span#queryresultlines').text(" " + result.lines + " lines, ");
        $('span#queryresultbytes').text(" " + result.bytes + " bytes");
      }
    }
    else {
      $('span#queryresult').hide();
    }
  }
}

/* query status auto-updates */

/*
var shibdata = {};
var shibselectedquery = null;
var shibselectedquery_dom = null;
 */
function check_running_query_status(event){ /* event object is not used */
  
};

function update_query(query){
  $.get('/status/' + query.queryid, function(data){
    var lastresult = query_last_result(query);
    var state = (lastresult && lastresult.state) || null;
    //TODO write!
  });
};

/* left pane interactions (user-operation interactions) */

/* test functions */

function shib_test_status_change(next_state) {
  if (next_state == 'not executed'){
    show_editbox_buttons(['execute_button']);
    change_editbox_querystatus_style('not executed');
  }
  else if (next_state == 'running'){
    show_editbox_buttons(['pause_button']);
    change_editbox_querystatus_style('running');
  }
  else if (next_state == 'executed'){
    show_editbox_buttons(['rerun_button', 'display_full_button', 'display_head_button', 'download_tsv_button', 'download_csv_button']);
    change_editbox_querystatus_style('executed', {lines:512, bytes:2049});
  }
  else if (next_state == 'error'){
    show_editbox_buttons(['rerun_button']);
    change_editbox_querystatus_style('error', {error:'hdfs volume full'});
  }
  else if (next_state == 're-running'){
    show_editbox_buttons(['pause_button', 'display_full_button', 'display_head_button', 'download_tsv_button', 'download_csv_button']);
    change_editbox_querystatus_style('re-running', {lines:1, bytes:50});
  }
}
