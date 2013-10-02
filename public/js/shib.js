var shibnotifications = [];
var shibdata = {};
var shibselectedquery = null;
var shibselectedquery_dom = null;

var shibdetailcontrol = false;

var shib_QUERY_STATUS_CHECK_INTERVAL = 5000;
var shib_QUERY_EDITOR_WATCHER_INTERVAL = 500;
var shib_NOTIFICATION_CHECK_INTERVAL = 100;
var shib_NOTIFICATION_DEFAULT_DURATION_SECONDS = 10;
var shib_RUNNING_QUERY_UPDATE_INTERVAL = 15000;

$(function(){
  if ($('input#detailcontrol').val() === 'true') {
    shibdetailcontrol = true;
  }

  load_tabs({callback:function(){
    follow_current_uri();
    setInterval(check_selected_running_query_state, shib_QUERY_STATUS_CHECK_INTERVAL);
    setInterval(show_notification, shib_NOTIFICATION_CHECK_INTERVAL);
    setInterval(update_running_queries, shib_RUNNING_QUERY_UPDATE_INTERVAL);
  }});
  
  //hover states on the static widgets
  $('ul.operationitems li').hover(
    function() { $(this).addClass('ui-state-hover'); }, 
    function() { $(this).removeClass('ui-state-hover'); }
  );

  if ($('select#table_dbname').size() > 0) {
    $('#tables_diag,#describe_diag')
      .css('text-decoration', 'line-through')
      .css('cursor', 'wait');
    load_database_list(function(){
      $('#tables_diag').click(function(event){show_tables_dialog();});
      $('#table_dbname').change(function(event){show_tables_dialog();});
      $('#describe_diag').click(function(event){show_describe_dialog();});
      $('#desc_dbname').change(function(event){show_describe_dialog();});
      $('#tables_diag,#describe_diag')
        .css('text-decoration', '')
        .css('cursor', 'pointer');
    });
  } else {
    $('#tables_diag').click(function(event){show_tables_dialog();});
    $('#describe_diag').click(function(event){show_describe_dialog();});
  }

  $('#new_button').click(initiate_mainview);
  $('#copy_button').click(copy_selected_query);
  $('#clip_button').click(clip_selected_query);
  $('#unclip_button').click(unclip_selected_query);

  $('#execute_button').click(execute_query);
  $('#giveup_button').click(giveup_query);
  $('#status_button').click(show_status_query);
  $('#delete_button').click(delete_query);
  $('#display_full_button').click(function(){show_result_query({range:'full'});});
  $('#display_head_button').click(function(){show_result_query({range:'head'});});
  $('#download_tsv_button').click(function(){download_result_query({format:'tsv'});});
  $('#download_csv_button').click(function(){download_result_query({format:'csv'});});

});

/* database list loading (just after page loading) */

$.template("databasesTemplate",
           '<option ${Selected}>${Dbname}</option>');
function load_database_list(callback) {
  $.get('/databases?=' + (new Date()).getTime(), function(data){
    if (data.length < 1) return;

    var defaultdb = $('select#table_dbname').data('defaultdb');
    $('select#table_dbname,select#desc_dbname').empty();
    $.tmpl('databasesTemplate',
           data.map(function(dbname){ return {Dbname:dbname, Selected:(defaultdb === dbname ? 'selected' : '')}; })
          ).appendTo('select#table_dbname');
    $.tmpl('databasesTemplate',
           data.map(function(dbname){ return {Dbname:dbname, Selected:(defaultdb === dbname ? 'selected' : '')}; })
          ).appendTo('select#desc_dbname');
    callback();
  });
};

/* basic data operations */

function set_execute_query_list(list) {
  if (! window.localStorage) return;
  window.localStorage.executeList = JSON.stringify(list);
};

function delete_execute_query_item(queryid) {
  if (! window.localStorage) return;
  window.localStorage.executeList = JSON.stringify(execute_query_list().filter(function(v){return v !== queryid;}));
};

function execute_query_list() {
  if (! window.localStorage) return [];
  var list = [];
  try {
    var listString = window.localStorage.executeList;
    if (listString && listString.length > 0)
      list = JSON.parse(listString);
  } catch (e) { set_execute_query_list([]); list = []; }
  return list;
};

function push_execute_query_list(queryid, refresh) {
  if (! window.localStorage) return;
  var list = execute_query_list();
  if (refresh)
    list = list.filter(function(v){return v !== queryid;});
  else if (list.filter(function(v){return v === queryid;}).length > 0)
    return;
  if (list.length > 10) list = list.slice(0,10);
  list.unshift(queryid);
  set_execute_query_list(list);
};

function set_bookmark_query_list(list) {
  if (! window.localStorage) return;
  window.localStorage.bookmark = JSON.stringify(list);
};

function delete_bookmark_query_list(queryid) {
  if (! window.localStorage) return;
  window.localStorage.bookmark = JSON.stringify(bookmark_query_list().filter(function(v){return v !== queryid;}));
};

function bookmark_query_list() {
  if (! window.localStorage) return [];
  var list = [];
  try {
    var listString = window.localStorage.bookmark;
    if (listString && listString.length > 0)
      list = JSON.parse(listString);
  } catch (e) { set_bookmark_query_list([]); list = []; }
  return list;
};

function exists_in_bookmark_query_list(queryid) {
  if (! window.localStorage) return false;
  return bookmark_query_list().filter(function(v){return v === queryid;}).length > 0;
};

function push_bookmark_query_list(queryid) {
  if (! window.localStorage) return;
  var list = bookmark_query_list().filter(function(v){return v !== queryid;});
  list.unshift(queryid);
  set_bookmark_query_list(list);
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
  $('#tablesdiag').dialog({modal:false, resizable:true, height:400, width:400, maxHeight:650, maxWidth:950});
  $('#tablesdiag .loadingimg').show();
  var dbname = null;
  if ($('#table_dbname').val()) {
    dbname = $('#table_dbname').val();
  }
  var get_path = (dbname ? '/tables?db=' + dbname : '/tables');
  $.get(get_path, function(data){
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
          var datahash = {key: node.data.key};
          if (dbname) { datahash.db = dbname; }
          node.appendAjax({
            url: '/partitions',
            data: datahash,
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
  $('#describediag').dialog({modal:false, resizable:true, height:400, width:400, maxHeight:650, maxWidth:950});
  $('#describediag .loadingimg').show();
  var dbname = null;
  if ($('#desc_dbname').val()) {
    dbname = $('#desc_dbname').val();
  }
  var get_path = (dbname ? '/tables?db=' + dbname : '/tables');
  $.get(get_path, function(data){
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
          var datahash = {key: node.data.key};
          if (dbname) { datahash.db = dbname; }
          node.appendAjax({
            url: '/describe',
            data: datahash,
            cache: false
          });
        }
      });
  });
};

$.template("detailStatusTemplate",
           '<table>' +
           '<tr><td>Job ID</td><td>${JobID}</td></tr>' +
           '<tr><td>State</td><td>${State}</td></tr>' +
           '<tr><td>Priority</td><td>${Priority}</td></tr>' +
           '<tr><td>URL</td><td><a href="${Url}">${Url}</a></td></tr>' +
           '<tr><td>Complete</td><td>Map:${MapComplete}, Reduce:${ReduceComplete}</td></tr>' +
           '</table>');
function show_status_dialog(target) {
  $('#detailstatus').empty().hide();
  $('#detailstatusdiag').dialog({modal:true, resizable:false, height:200, width:600, maxHeight:200, maxWidth:950});
  $('#detailstatusdiag .loadingimg').show();
  $.ajax({
    url: '/detailstatus/' + target.queryid,
    type: 'GET',
    cache: false,
    error: function(jqXHR, textStatus, err) {
      console.log(jqXHR);
      console.log(textStatus);
      var msg = null;
      try { msg = JSON.parse(jqXHR.responseText).message; }
      catch (e) { msg = jqXHR.responseText; }
      show_error('Failed to get detail status', msg);
    },
    success: function(state) {
      /*
       var returnedValus = {
         jobid: 'job_201304011701_1912',
         name: 'shib-3578d8d4f5a1812de7a7714f5b108776',
         priority: 'NORMAL',
         state: 'RUNNING',
         trackingURL: 'http://master.hadoop.local:50030/jobdetails.jsp?jobid=job_201304011701_1912',
         startTime: 'Thu Apr 11 2013 16:06:40 (JST)',
         mapComplete: 89,
         reduceComplete: 29,
         hiveQueryId: 'hive_20130411160606_46b1b669-3a64-4174-899e-bb1bf53e90db',
         hiveQueryString: 'SELECT ...'
       };
       */
      $.tmpl("detailStatusTemplate",[
        {
          JobID: state['jobid'], State: state['state'], Priority: state['priority'],
          Url: state['trackingURL'],
          MapComplete: String(state['mapComplete'] || 0) + '%',
          ReduceComplete: String(state['ReduceComplete'] || 0) + '%'
        }
      ]).appendTo('#detailstatus');
      $('#detailstatusdiag .loadingimg').hide();
      $('#detailstatus').show();
    }
  });
}

/* right pane operations */

function update_tabs(reloading) {
  if (reloading) {
    $('#listSelector').tabs('destroy');
    if (window.localStorage) {
      $('#tab-yours').accordion('destroy');
      $('#tab-bookmark').accordion('destroy');
    }
    $('#tab-history').accordion('destroy');
  }

  if (window.localStorage) {
    update_yours_tab();
    update_bookmark_tab();
    $("#tab-yours").accordion({header:"h3", autoHeight:false});
    $("#tab-bookmark").accordion({header:"h3", autoHeight:false});
  }
  else {
    $('#index-yours,#tab-yours').remove();
    $('#index-bookmark,#tab-bookmark').remove();
  }

  update_history_tab(reloading);
  $("#tab-history").accordion({header:"h3", autoHeight:false});

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
    shibdata.history = data.history; /* ["201302", "201301", "201212", "201211"] */
    shibdata.history_ids = data.history_ids; /* {"201302":[query_ids], "201301":[query_ids], ...} */
    shibdata.query_cache = {};
    shibdata.query_state_cache = {};
    shibdata.result_cache = {};

    /* query_ids == sum of values of history_ids */
    load_queries(data.query_ids, function(err, queries){
      var resultids = [];
      queries.forEach(function(v){
        if (v.results && v.results.length > 0)
          resultids = resultids.concat(v.results.map(function(r){return r && r.resultid;}));
      });
      resultids = resultids.concat(execute_query_list()).concat(bookmark_query_list());
      if (resultids.length < 1) {
        callback();
        return;
      }
      load_results(resultids, function(err, results){callback();});
    });
  });
};

$.template("queryItemTemplate",
           '<div><div class="queryitem" id="query-${QueryId}">' +
           '  <div class="queryitem_information"><table><tr>' +
           '    <td width="80%">${Information}</td>' +
           '    <td width="20%" style="text-align: right;"><a href="/q/${QueryKey}">URL</a></td>' +
           '  </tr></table></div>' +
           '  <div class="queryitem_statement">${Statement}</div>' +
           '  <div class="queryitem_status">' +
           '    <span class="status_${Status}">${Status}</span>' +
           '    <span class="queryitem_etc">${Etc}</span>' +
           '    ' +
           '  </div>' +
           '</div></div>');

function create_queryitem_object(queryid, id_prefix){
  var query = shibdata.query_cache[queryid];
  if (! query)
    return '';
  var lastresult = query_last_result(query);
  var executed_at = (lastresult && lastresult.executed_at) || '-';
  return {
    QueryKey: query.queryid,
    QueryId: (id_prefix || '') + query.queryid,
    Information: executed_at,
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

function update_bookmark_tab(){
  $('#tab-bookmark')
    .empty()
    .append('<div><h3><a href="#">bookmark</a></h3><div id="bookmark-idlist"></div></div>');
  if (bookmark_query_list().length > 0)
    $.tmpl("queryItemTemplate",
           bookmark_query_list().map(function(id){return create_queryitem_object(id, 'bookmark-');})
          ).appendTo('#tab-bookmark div div#bookmark-idlist');
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
  var dom_id_regex = /^query-(yours|bookmark|history)-([0-9a-f]+)$/;
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
  update_desceditor(true);
  update_editbox(null, 'not executed');
  update_history_by_query(null);
};

function copy_selected_query(event) { /* event not used */
  var querystring = shibselectedquery.querystring;
  deselect_and_new_query();
  update_queryeditor(true, querystring);
  update_desceditor(true);
  update_editbox(null, 'not executed');
  update_history_by_query(null);
};

function clip_selected_query(event) { /* event not used */
  var clip_query_id = shibselectedquery.queryid;
  push_bookmark_query_list(clip_query_id);
  load_tabs({
    reload:true,
    callback:function(){
      $("#listSelector").tabs('option', 'selected', 1);
    }
  });
  update_editbox(shibselectedquery);
};

function unclip_selected_query(event) { /* event not used */
  var unclip_query_id = shibselectedquery.queryid;
  delete_bookmark_query_list(unclip_query_id);
  load_tabs({
    reload:true,
    callback:function(){
      $("#listSelector").tabs('option', 'selected', 1);
    }
  });
  update_editbox(shibselectedquery);
};


function update_mainview(query){
  shibselectedquery = query;
  update_queryeditor(false, query.querystring);
  update_desceditor(false);
  update_editbox(query);
};

function update_desceditor(editable){
  //datas['query_title'] = $('#query_title').val();
  //datas['query_name'] = $('#query_name').val();
  //datas['query_desc'] = $('#query_desc').val();
    var query_title= $('#query_title');
    var query_name= $('#query_name');
    var query_desc= $('#query_desc');
    var elms =[query_title,query_name,query_desc];
    if (editable){
	for (var i=0;i < elms.length;i++){
	    elms[i].attr('readonly', false).removeClass('readonly');
	}

    }else{
	for (var i=0;i < elms.length;i++){
	    elms[i].attr('readonly', true).addClass('readonly');
	}
    }
}
function update_queryeditor(editable, querystring) {
  var editor = $('#queryeditor');
  editor.val(querystring);
  if (editable)
    editor.attr('readonly', false).removeClass('readonly');
  else
    editor.attr('readonly', true).addClass('readonly');
};

function update_editbox(query, optional_state) {
  if (query) {
    $('#copy_button').show();
    if (exists_in_bookmark_query_list(query.queryid)) {
      $('#clip_button').hide();
      $('#unclip_button').show();
    } else {
      $('#clip_button').show();
      $('#unclip_button').hide();
    }
  } else {
    $('#copy_button,#clip_button,#unclip_button').hide();
  }

  var state = optional_state || query_current_state(query);
  switch (state) {
  case 'not executed':
  case undefined:
  case null:
    show_editbox_buttons(['execute_button']);
    change_editbox_querystatus_style('not executed');
    break;
  case 'running':
    if (shibdetailcontrol) {
      show_editbox_buttons(['giveup_button', 'status_button']);
    }
    else {
      show_editbox_buttons(['giveup_button']);
    }
    change_editbox_querystatus_style('running');
    break;
  case 'executed':
  case 'done':
    /*
    show_editbox_buttons(['rerun_button', 'delete_button', 'display_full_button', 'display_head_button',
                          'download_tsv_button', 'download_csv_button']);
     */
    show_editbox_buttons(['delete_button', 'display_full_button', 'display_head_button',
                          'download_tsv_button', 'download_csv_button']);
    change_editbox_querystatus_style('executed', query_last_result(query));
    break;
  case 'error':
    // show_editbox_buttons(['rerun_button', 'delete_button']);
    show_editbox_buttons(['delete_button']);
    change_editbox_querystatus_style('error', query_last_result(query));
    break;
  default:
    show_error('UI Bug', 'unknown query status:' + state, 5, query);
  }
}

function show_editbox_buttons(buttons){
  var allbuttons = [
    'execute_button', 'giveup_button', 'status_button', 'delete_button',
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
  if (state === 'done')
    state = 'executed';

  if (allstates[state]) {
    var allclasses = 'status_not_executed status_running status_executed status_error status_re-running';
    $('span#querystatus')
      .removeClass(allclasses)
      .addClass((allstates[state]).classname)
      .text(state);

    if (allstates[state]['result'] && result) {
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
      $('#queryresult').hide();
    }
  }
}

/* query and result load/reload/caching */

function load_queries(queryids, callback){
  if (queryids.length < 1) {
    callback(null, []); return;
  }
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
        callback(null, data.queries);
    }
  });
};

function load_results(resultids, callback){
  if (resultids.length < 1) {
    callback(null, []); return;
  }
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
        callback(null, data.results);
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
  datas={};
  datas['querystring'] = $('#queryeditor').val();
  datas['query_title'] = $('#query_title').val();
  datas['query_name'] = $('#query_name').val();
  datas['query_desc'] = $('#query_desc').val();
  $.ajax({
    url: '/execute',
    type: 'POST',
    dataType: 'json',
    data: datas,
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
      load_results(query.results.map(function(v){return v.resultid;}), function(err){
        update_mainview(query);
        load_tabs({reload:true});
      });
    }
  });
};

function show_status_query(event) {
  if (! shibdetailcontrol)
    return;
  if (! shibselectedquery)
    return;
  show_status_dialog(shibselectedquery);
}

function delete_query(event) {
  if (! shibselectedquery)
    return;
  var target = shibselectedquery;
  $.ajax({
    url: '/delete',
    type: 'POST',
    dataType: 'json',
    data: {queryid: target.queryid},
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
