var http = require('http')
  , child_process = require('child_process');

var cheerio = require('cheerio');

var MRv1Client = exports.MRv1Client = function(host, port, mapred){
  this.host = host; // jobtracker.hostname.local
  this.port = port; // 50030
  this.mapred = mapred;
};

MRv1Client.prototype.listAll = function(callback){
  this.request('/jobtracker.jsp', function(err, html){
    if (err) { callback(err); return; }
    var $ = cheerio.load(html);
    var jobs = [];
    ['h2#running_jobs', 'h2#completed_jobs', 'h2#failed_jobs'].forEach(function(marker){
      if ($(marker).next().is('table') && $(marker).next().find('tr td').text() !== 'none') {
        $(marker).next().find('tbody tr').each(function(i, e){
          var row = $(e).find('td');
          jobs.push({ jobid: row.eq(0).text(), name: row.eq(3).text(), priority: row.eq(1).text() });
        });
      }
    });
    callback(null, jobs);
  });
};

MRv1Client.prototype.detail = function(jobid, opts, callback){
  var self = this;
  var path = '/jobdetails.jsp?jobid=' + jobid;
  this.request(path, function(err, html){
    if (err) { callback(err); return; }
    var job = {
      jobid: jobid,
      name: '',
      priority: opts.priority,
      state: '',
      trackingURL: 'http://' + self.host + ':' + self.port + path,
      startTime: '',
      mapComplete: 0,
      reduceComplete: 0
    };
    var $ = cheerio.load(html);

    $('body').html().split('\n').forEach(function(line){
      var match = /^<b>(.+):<\/b> (.*)<br>$/.exec(line);
      if (match) {
        if (match[1] === 'Job Name') {
          job['name'] = match[2];
        } else if (match[1] === 'Status') {
          job['state'] = match[2];
        } else if (match[1] === 'Started at') {
          job['startTime'] = match[2];
        }
      }
    });

    var progress = $('table').first();
    job['mapComplete'] = parseInt(progress.children().eq(1).find('td').first().text());
    job['reduceComplete'] = parseInt(progress.children().eq(2).find('td').first().text());

    callback(null, job);
  });
};

MRv1Client.prototype.kill = function(jobid, callback){
  var command = this.mapred + " job -kill " + jobid;
  child_process.exec(command, function(err, stdout, stderr){
    callback(err);
  });
};

MRv1Client.prototype.request = function(path, callback){
  var options = {
    host: this.host,
    port: this.port,
    path: path,
    method: 'GET'
  };
  var cb = function(res){
    if (res.statusCode < 200 || res.statusCode >= 300) {
      callback({message: "JobTracker returns response code " + res.statusCode});
      return; 
    }
    res.setEncoding('utf8');
    var html = '';
    res.on('data', function(chunk){
      html += chunk;
    });
    res.on('end', function(){
      callback(null, html);
    });
  };
  var errcb = function(e){ callback(e, null); };
  http.request(options, cb).on('error', errcb).end();
};
