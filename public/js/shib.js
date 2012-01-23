google.load("visualization", "1", {packages:["corechart"]});

var shibnotifications = [];
var shibdata = {};
var shibselectedquery = null;
var shibselectedquery_dom = null;

var shib_QUERY_STATUS_CHECK_INTERVAL = 5000;
var shib_QUERY_EDITOR_WATCHER_INTERVAL = 500;
var shib_NOTIFICATION_CHECK_INTERVAL = 100;
var shib_NOTIFICATION_DEFAULT_DURATION_SECONDS = 10;
var shib_RUNNING_QUERY_UPDATE_INTERVAL = 15000;

$(function(){
  load_tabs({callback:function(){
    follow_current_uri();
    setInterval(queryeditor_watcher(), shib_QUERY_EDITOR_WATCHER_INTERVAL);
    setInterval(check_selected_running_query_state, shib_QUERY_STATUS_CHECK_INTERVAL);
    setInterval(show_notification, shib_NOTIFICATION_CHECK_INTERVAL);
    setInterval(update_running_queries, shib_RUNNING_QUERY_UPDATE_INTERVAL);
  }});
  
  //hover states on the static widgets
  $('ul.operationitems li').hover(
    function() { $(this).addClass('ui-state-hover'); }, 
    function() { $(this).removeClass('ui-state-hover'); }
  );

  $('#tables_diag').click(function(event){show_tables_dialog();});
  $('#describe_diag').click(function(event){show_describe_dialog();});

  $('#new_button').click(initiate_mainview);
  $('#copy_button').click(copy_selected_query);

  $('#execute_button').click(execute_query);
  $('#giveup_button').click(giveup_query);
  $('#rerun_button').click(rerun_query);
  $('#delete_button').click(delete_query);
  $('#display_full_button').click(function(){show_result_query({range:'full'});});
  $('#display_head_button').click(function(){show_result_query({range:'head'});});
  $('#download_tsv_button').click(function(){download_result_query({format:'tsv'});});
  $('#download_csv_button').click(function(){download_result_query({format:'csv'});});

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

function execute_query_list() {
  if (! window.localStorage)
    return [];
  var list = [];
  try {
    var listString = window.localStorage.executeList;
    if (listString && listString.length > 0)
      list = JSON.parse(listString);
  }
  catch (e) {
    set_execute_query_list([]);
    list = [];
  }
  return list;
};

function set_execute_query_list(list) {
  if (! window.localStorage)
    return;
  window.localStorage.executeList = JSON.stringify(list);
};

function delete_execute_query_item(queryid) {
  if (! window.localStorage)
    return;
  window.localStorage.executeList = JSON.stringify(execute_query_list().filter(function(v){return v !== queryid;}));
};

function push_execute_query_list(queryid, refresh) {
  if (! window.localStorage)
    return;
  var list = execute_query_list();
  if (refresh)
    list = list.filter(function(v){return v !== queryid;});
  else if (list.filter(function(v){return v === queryid;}).length > 0)
    return;
  if (list.length > 4) {
    list = list.slice(0,4);
  }
  list.unshift(queryid);
  set_execute_query_list(list);
};

function query_last_result(query) {
  var obj = null;
  if (query && query.results && query.results.length > 0 && query.results[query.results.length - 1])
    if ((obj = shibdata.result_cache[query.results[query.results.length - 1].resultid]) !== null)
      return obj;
  return null;
};
function query_second_last_result(query) {
  var obj = null;
  if (query && query.results && query.results.length > 1 && query.results[query.results.length - 2])
    if ((obj = shibdata.result_cache[query.results[query.results.length - 2].resultid]) !== null)
      return obj;
  return null;
};
function query_last_done_result(query) {
  var last = query_last_result(query);
  if (last && last.state == 'done')
    return last;
  return query_second_last_result(query);
}

function query_result_schema_label(result){
  return 'fields: ' + result.schema.map(function(field){return field.name + '(' + field.type + ')';}).join(', ');
};

function query_current_state(query) {
  if (!query)
    return null;
  if (query && (! query.queryid))
    show_error('UI Bug', 'query id unknown', 5, query);

  if (shibdata.query_state_cache[query.queryid])
    return shibdata.query_state_cache[query.queryid];

  var state = null;
  var lastresult = query_last_result(query);
  if (! lastresult)
    state = 'running';
  else if (lastresult.state === 'running') {
    var secondlast = query_second_last_result(query);
    if (secondlast && secondlast.state === 'done')
      state = 're-running';
    else
      state = 'running';
  }
  else if (lastresult.state === 'error')
    state = 'error';
  else
    state = 'executed';

  shibdata.query_state_cache[query.queryid] = state;
  return state;
};

function detect_keyword_placeholders(querystring, opts) {
  var q = querystring;
  if (q.match(/__KEY__/) && q.match(/__KEY\d__/))
    if (! opts.quiet)
      show_error('Query Error', 'Cannot use both default single placeholder __KEY__ and sequencial placeholders such as __KEY1__');
  if (q.match(/__KEY\d{2,}__/))
    if (! opts.quiet)
      show_error('Query Error', 'Cannot use 10 or more sequencial placeholders such as __KEY10__');

  if (q.match(/__KEY\d__/)) {
    var re = /__KEY(\d)__/g;
    var matched;
    var max_seq = 0;
    var exists_seq = {};
    while ((matched = re.exec(q)) != null) {
      exists_seq[parseInt(matched[1])] = true;
      if (matched[1] > max_seq)
        max_seq = matched[1];
    }
    for (var i = 0; i <= max_seq; i++) {
      if (! exists_seq[i])
        if (! opts.quiet)
          show_error('Query Warning', 'Query has skipping sequencial placeholder number');
    }
    return parseInt(max_seq) + 1;
  }
  if (q.match(/__KEY__/))
    return 1;
  return 0;
};

function timelabel_elapsed(completed_at, executed_at){
  if (!completed_at || !executed_at)
    return 'unknown times';
  var seconds = Math.floor(((new Date(completed_at)) - (new Date(executed_at))) / 1000);
  if (seconds < 60)
    return seconds + ' seconds';
  var minutes = Math.floor(seconds / 60);
  if (minutes < 60)
    return minutes + ' minutes';
  return Math.floor(minutes / 60) + ' hours';
};

/* uri and history operation */

function follow_current_uri() {
  if (window.location.pathname.indexOf('/q/') != 0)
    return;
  var queryid = window.location.pathname.substring('/q/'.length);
  if (! /^[0-9a-z]{32}$/.exec(queryid)) // queryid is md5 (16bytes) hexdigest (32chars)
    return;
  var query = shibdata.query_cache[queryid];
  if (! query) {
    $.ajax({
      url: '/query/' + queryid,
      type: 'GET',
      error: function(jqXHR, textStatus, errorThrown){
        show_error('Unknown query id', 'cannot get query object with specified id', 10);
      },
      success: function(data, textStatus, jqXHR){
        query = data;
        shibdata.query_cache[queryid] = query;
        var resultids = data.results.map(function(v){return v.resultid;});
        $.ajax({
          url: '/results',
          type: 'POST',
          dataType: 'json',
          data: {ids: resultids},
          success: function(data){
            data.results.forEach(function(result1){
              if (! result1)
                return;
              shibdata.result_cache[result1.resultid] = result1;
            });
            update_mainview(query);
          }
        });
      }
    });
    return;
  }
  else
    update_mainview(query);
};

function update_history_by_query(query) {
  if (! window.history.pushState ) // if pushState not ready
    return;
  if (query === null) {
    window.history.pushState('','', '/');
    return;
  }
  window.history.pushState(query.queryid, '', '/q/' + query.queryid);
};

window.addEventListener("popstate", function (event) {
  if (event.state === null || event.state === undefined || event.state.length < 32)
    return;
  var query = shibdata.query_cache[event.state];
  if (! query) {
    show_error('UI BUG', 'unknown queryid from history event.state', 10);
    return;
  }
  update_mainview(query);
}, false);

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
  shib_current_notification_counter = ( next.duration || shib_NOTIFICATION_DEFAULT_DURATION_SECONDS ) * 10;
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

/* dialog */

function show_tables_dialog() {
  $('#tables')
    .dynatree('destroy')
    .empty()
    .hide();
  $('#tablesdiag').dialog({modal:true, resizable:true, height:400, width:400, maxHeight:650, maxWidth:950});
  $('#tablesdiag .loadingimg').show();
  $.get('/tables', function(data){
    $('#tablesdiag .loadingimg').hide();
    $('#tables')
      .show()
      .dynatree({
        children: data.map(function(v){return {title: v, key: v, isFolder: true, isLazy: true};}),
        autoFocus: false,
        autoCollapse: true,
        clickFolderMode: 2,
        activeVisible: false,
        onLazyRead: function(node){
          node.appendAjax({
            url: '/partitions',
            data: {key: node.data.key},
            cache: false
          });
        }
      });
  });
};

function show_describe_dialog() {
  $('#describes')
    .dynatree('destroy')
    .empty()
    .hide();
  $('#describediag').dialog({modal:true, resizable:true, height:400, width:400, maxHeight:650, maxWidth:950});
  $('#describediag .loadingimg').show();
  $.get('/tables', function(data){
    $('#describediag .loadingimg').hide();
    $('#describes')
      .show()
      .dynatree({
        children: data.map(function(v){return {title: v, key: v, isFolder: true, isLazy: true};}),
        autoFocus: false,
        autoCollapse: true,
        clickFolderMode: 2,
        activeVisible: false,
        onLazyRead: function(node){
          node.appendAjax({
            url: '/describe',
            data: {key: node.data.key},
            cache: false
          });
        }
      });
  });
};

/* right pane operations */

function update_tabs(reloading) {
  if (reloading) {
    $('#listSelector').tabs('destroy');
    if (window.localStorage)
      $('#tab-yours').accordion('destroy');
    $('#tab-history').accordion('destroy');
    $('#tab-keywords').accordion('destroy');
  }

  if (window.localStorage) {
    update_yours_tab();
    $("#tab-yours").accordion({header:"h3", autoHeight:false});
  }
  else {
    $('#index-yours').remove();
    $('#tab-yours').remove();
  }

  update_history_tab(reloading);
  $("#tab-history").accordion({header:"h3", autoHeight:false});

  update_keywords_tab(reloading);
  $("#tab-keywords").accordion({header:"h3", autoHeight:false});
  $("#listSelector").tabs();

  $('.queryitem').click(select_queryitem);
};

function load_tabs(opts) {
  var callback = function() {
    update_tabs(opts.reload);
    if (opts.callback)
      opts.callback();
  };
  $.getJSON('/summary_bulk', function(data){
    shibdata.history = data.history;
    shibdata.keywords = data.keywords;
    shibdata.history_ids = data.history_ids;
    shibdata.keyword_ids = data.keyword_ids;
    shibdata.query_cache = {};
    shibdata.query_state_cache = {};
    shibdata.result_cache = {};

    load_queries(data.query_ids, function(queries){
      var resultids = [];
      queries.forEach(function(v){
        if (v.results && v.results.length > 0)
          resultids = resultids.concat(v.results.map(function(r){return r && r.resultid;}));
      });
      var yours = execute_query_list();
      if (yours.length > 0)
        resultids = resultids.concat(execute_query_list());
      if (resultids.length < 1) {
        callback();
        return;
      }
      load_results(resultids, function(results){callback();});
    });
  });
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
  var lastresult = query_last_result(query);
  var executed_at = (lastresult && lastresult.executed_at) || '-';
  var keyword_primary = (query.keywords && query.keywords.length > 0 && query.keywords[0]) || '-';
  return {
    QueryId: (id_prefix || '') + query.queryid,
    Information: executed_at + ', ' + keyword_primary,
    Statement: query.querystring,
    Status: query_current_state(query),
    Etc: lastresult ?
      (timelabel_elapsed(lastresult.completed_at, lastresult.executed_at) +
       ((lastresult && lastresult.bytes && lastresult.lines &&
         (', ' + lastresult.bytes + ' bytes, ' + lastresult.lines + ' lines')) || '')
      ) : 'not started'
  };
};

function update_yours_tab(){
  $('#tab-yours')
    .empty()
    .append('<div><h3><a href="#">your queries</a></h3><div id="yours-idlist"></div></div>');
  if (execute_query_list().length > 0)
    $.tmpl("queryItemTemplate",
           execute_query_list().map(function(id){return create_queryitem_object(id, 'yours-');})
          ).appendTo('#tab-yours div div#yours-idlist');
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

function deselect_and_new_query(quiet){
  release_selected_query();
  update_editbox(null);
  if (! quiet)
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
  var dom_id_regex = /^query-(yours|keyword|history)-([0-9a-f]+)$/;
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
  update_history_by_query(query);
  update_mainview(query);
};

/* left pane view updates */

function initiate_mainview(event, quiet) { /* event not used */
  deselect_and_new_query(quiet);
  update_queryeditor(true, '');
  update_keywordbox(true, 0);
  update_editbox(null, 'not executed');
  update_history_by_query(null);
};

function copy_selected_query(event) { /* event not used */
  var querystring = shibselectedquery.querystring;
  var keywordlist = shibselectedquery.keywords;
  deselect_and_new_query();
  update_queryeditor(true, querystring);
  update_editbox(null, 'not executed');
  update_keywordbox(true, detect_keyword_placeholders(querystring), keywordlist);
  update_history_by_query(null);
};

function update_mainview(query){
  shibselectedquery = query;
  update_queryeditor(false, query.querystring);
  update_keywordbox(false, query.keywords.length, query.keywords);
  update_editbox(query);
};

function queryeditor_watcher(){
  var pre_querystring_value = '';
  return function(){
    if ($('#queryeditor').attr('readonly'))
      return;
    if (pre_querystring_value == $('#queryeditor').val())
      return;
    pre_querystring_value = $('#queryeditor').val();
    update_keywordbox(true, detect_keyword_placeholders($('#queryeditor').val()), []);
  };
};

function update_queryeditor(editable, querystring) {
  var editor = $('#queryeditor');
  editor.val(querystring);
  if (editable)
    editor.attr('readonly', false).removeClass('readonly');
  else
    editor.attr('readonly', true).addClass('readonly');
};

function update_keywordbox(editable, keywords, keywordlist) {
  if (keywords < 1) {
    $('#keywordbox div input').val('').attr('readonly', true).addClass('readonly');
    $('#keywordbox div .keywordname').hide();
    $('#keywordbox div').hide();
    $('#keywordbox').hide();
    return;
  }
  $('#keywordbox').show();
  if (! keywordlist)
    keywordlist = [];
  for(var i = 0; i < 10; i++) {
    if (i < keywords) {
      $('#keyword' + i + 'area').show();
      $('#keyword' + i + 'area .keywordname').show();
      var input = $('#keyword' + i);
      input.show();
      if (keywordlist[i] && keywordlist[i].length > 0)
        input.val(keywordlist[i]);
      if (editable)
        input.removeClass('readonly').attr('readonly', false);
      else
        input.addClass('readonly').attr('readonly', true);
    }
    else {
      $('#keyword' + i).val('').attr('readonly', true).addClass('readonly');
      $('#keyword' + i + 'area .keywordname').hide();
      $('#keyword' + i + 'area').hide();
    }
  }
};

function update_editbox(query, optional_state) {
  if (query)
    $('#copy_button').show();
  else
    $('#copy_button').hide();

  var state = optional_state || query_current_state(query);
  switch (state) {
  case 'not executed':
  case undefined:
  case null:
    show_editbox_buttons(['execute_button']);
    change_editbox_querystatus_style('not executed');
    break;
  case 'running':
    show_editbox_buttons(['giveup_button']);
    change_editbox_querystatus_style('running');
    break;
  case 'executed':
  case 'done':
    show_editbox_buttons(['rerun_button', 'delete_button', 'display_full_button', 'display_head_button',
                          'download_tsv_button', 'download_csv_button']);
    change_editbox_querystatus_style('executed', query_last_result(query));
    break;
  case 'error':
    show_editbox_buttons(['rerun_button', 'delete_button']);
    change_editbox_querystatus_style('error', query_last_result(query));
    break;
  case 're-running':
    show_editbox_buttons(['giveup_button', 'display_full_button', 'display_head_button',
                          'download_tsv_button', 'download_csv_button']);
    change_editbox_querystatus_style('re-running', query_last_done_result(query));
    break;
  default:
    show_error('UI Bug', 'unknown query status:' + state, 5, query);
  }
}

function show_editbox_buttons(buttons){
  var allbuttons = [
    'execute_button', 'giveup_button', 'rerun_button', 'delete_button',
    'display_full_button', 'display_head_button', 'download_tsv_button', 'download_csv_button'
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
    're-running':{classname:'status_re-running', result:true}
  };
  var allclasses = 'status_not_executed status_running status_executed status_error status_re-running';
  if (state === 'done')
    state = 'executed';

  if (allstates[state]) {
    $('span#querystatus')
      .removeClass(allclasses)
      .addClass((allstates[state]).classname)
      .text(state);

    if ((allstates[state]).result && result) {
      $('#queryresult').show();
      if (result.error) {
        $('span#queryresultlines').text(result.error);
        $('span#queryresultbytes').text("");
        $('#queryresultelapsed').text(timelabel_elapsed(result.completed_at, result.executed_at));
      }
      else {
        $('span#queryresultlines').text(" " + result.lines + " lines, ");
        $('span#queryresultbytes').text(" " + result.bytes + " bytes");
        $('#queryresultelapsed').text(timelabel_elapsed(result.completed_at, result.executed_at));
        $('#queryresultschema').text(query_result_schema_label(result));
      }
    }
    else {
      $('span#queryresult').hide();
    }
  }
}

/* query and result load/reload/caching */

function load_queries(queryids, callback){
  $.ajax({
    url: '/queries',
    type: 'POST',
    dataType: 'json',
    data: {ids: queryids},
    success: function(data){
      data.queries.forEach(function(query1){
        shibdata.query_cache[query1.queryid] = query1;
      });
      if (callback)
        callback(data.queries);
    }
  });
};

function load_results(resultids, callback){
  $.ajax({
    url: '/results',
    type: 'POST',
    dataType: 'json',
    data: {ids: resultids},
    success: function(data){
      data.results.forEach(function(result1){
        if (! result1)
          return;
        shibdata.result_cache[result1.resultid] = result1;
      });
      if (callback)
        callback(data.results);
    }
  });
};

/* query status auto-updates */

function check_selected_running_query_state(event){ /* event object is not used */
  if (! shibselectedquery)
    return;
  var s = query_current_state(shibselectedquery);
  if (s == 'running' || s == 're-running')
    update_query(shibdata.query_cache[shibselectedquery.queryid]);
};

function update_query_display(query) {
  update_mainview(query);
  show_info('Query state updated', '', 5);
  load_tabs({reload:true});
};

function update_query(query){
  if (! query)
    return;
  $.get('/status/' + query.queryid, function(data){
    if (query_current_state(query) == data)
      return;

    shibdata.query_state_cache[query.queryid] = data;

    $.get('/query/' + query.queryid, function(new_query){
      shibdata.query_cache[new_query.queryid] = new_query;
      if (new_query.results.length > 0) {
        $.get('/lastresult/' + new_query.queryid, function(new_result){
          shibdata.result_cache[new_result.resultid] = new_result;
          update_query_display(new_query);
        });
      }
      else {
        update_query_display(new_query);
      }
    });
  });
};

$.template("runningsTemplate",
           '<div><a href="/q/${QueryId}">${QueryId}</a> ${Runnings}</div>');

function update_running_queries(event){
  $.get('/runnings', function(data){
    $('#runnings').empty();
    if (data.length < 1) {
      $('<div>no running queries</div>').appendTo('#runnings');
      return;
    }
    $('#runnings').show();
    $.tmpl("runningsTemplate",
           data.map(function(pair){return {QueryId: pair[0], Runnings: pair[1]};})
          ).appendTo('#runnings');
  });
};

/* left pane interactions (user-operation interactions) */

function execute_query() {
  if (shibselectedquery) {
    show_error('UI Bug', 'execute_query should be enable with not-saved-query objects');
    return;
  }
  var querystring = $('#queryeditor').val();
  var keywordPlaceHolders = detect_keyword_placeholders(querystring, {quiet:true});
  var keywords = [];
  for(var i = 0; i < keywordPlaceHolders; i++) {
    var key = $('#keyword' + i).val();
    if (key && key.length > 0)
      keywords.push(key);
  }
  if (keywordPlaceHolders !== keywords.length) {
    show_error('Invalid Keywords', 'Blank keyword is not allowed');
    return;
  }

  $.ajax({
    url: '/execute',
    type: 'POST',
    dataType: 'json',
    data: {querystring: querystring, keywords: keywords},
    error: function(jqXHR, textStatus, err){
      console.log(jqXHR);
      console.log(textStatus);
      var msg = null;
      try {
        msg = JSON.parse(jqXHR.responseText).message;
      }
      catch (e) {
        msg = jqXHR.responseText;
      }
      show_error('Cannot Execute Query', msg);
    },
    success: function(query){
      show_info('Query now waiting to run', '');
      shibdata.query_cache[query.queryid] = query;
      update_mainview(query);
      if (window.localStorage) {
        push_execute_query_list(query.queryid);
      }
      update_history_by_query(query);
      load_tabs({reload:true});
    }
  });
};

function giveup_query() {
  if (! shibselectedquery) {
    show_error('UI Bug', 'giveup_query should be enable with non-saved-query objects');
    return;
  }
  $.ajax({
    url: '/giveup',
    type: 'POST',
    dataType: 'json',
    data: {queryid: shibselectedquery.queryid},
    error: function(jqXHR, textStatus, err){
      console.log(jqXHR);
      console.log(textStatus);
      var msg = null;
      try {
        msg = JSON.parse(jqXHR.responseText).message;
      }
      catch (e) {
        msg = jqXHR.responseText;
      }
      show_error('Cannot GiveUp Query', msg);
    },
    success: function(query){
      show_info('Query gived-up', '');
      shibdata.query_cache[query.queryid] = query;
      shibdata.query_state_cache[query.queryid] = 'error';
      load_results(query.results.map(function(v){return v.resultid;}), function(){
        update_mainview(query);
        load_tabs({reload:true});
      });
    }
  });
};

function rerun_query() {
  if (! shibselectedquery) {
    show_error('UI Bug', 'rerun_query should not be enable with non-saved-query objects');
    return;
  }
  $.ajax({
    url: '/refresh',
    type: 'POST',
    dataType: 'json',
    data: {queryid: shibselectedquery.queryid},
    error: function(jqXHR, textStatus, err){
      console.log(jqXHR);
      console.log(textStatus);
      var msg = null;
      try {
        msg = JSON.parse(jqXHR.responseText).message;
      }
      catch (e) {
        msg = jqXHR.responseText;
      }
      show_error('Cannot ReRun Query', msg);
    },
    success: function(query){
      show_info('Query now waiting to re-run', '');
      shibdata.query_cache[query.queryid] = query;
      shibdata.query_state_cache[query.queryid] = 're-running';
      update_mainview(query);
      if (window.localStorage) {
        push_execute_query_list(query.queryid, true); // with refreshing
      }
      load_tabs({reload:true});
    }
  });
};

function delete_query(event) {
  if (! shibselectedquery)
    return;
  var target = shibselectedquery;
  var targetkeyword = (target.keywords && target.keywords.length > 0) ? target.keywords[0] : null;
  $.ajax({
    url: '/delete',
    type: 'POST',
    dataType: 'json',
    data: {queryid: target.queryid, keyword: targetkeyword},
    error: function(jqXHR, textStatus, err){
      console.log(jqXHR);
      console.log(textStatus);
      var msg = null;
      try {
        msg = JSON.parse(jqXHR.responseText).message;
      }
      catch (e) {
        msg = jqXHR.responseText;
      }
      show_error('Failed to delete query', msg);
    },
    success: function(data){
      show_info('Selected query successfully deleted', '');
      initiate_mainview(null, true);
      delete_execute_query_item(target.queryid);
      load_tabs({reload:true});
    }
  });
};

function show_result_query(opts) { /* opts: {range:full/head} */
  //TODO show circular 'loading' icon ?
  var size = 'full';
  var height = 400;
  var width = 600;
  if (opts.range == 'head'){
    size = 'head';
    height = 200;
  }
  $.get('/show/' + size + '/' + query_last_done_result(shibselectedquery).resultid, function(data){
    $('pre#resultdisplay').text(data);
    $('#resultdiag').dialog({modal:true, resizable:true, height:400, width:600, maxHeight:650, maxWidth:950});
  });
};

function download_result_query(opts) { /* opts: {format:tsv/csv} */
  var format = 'tsv';
  if (opts.format == 'csv') {
    format = 'csv';
  }
  window.location = '/download/' + format + '/' + query_last_done_result(shibselectedquery).resultid;
};
