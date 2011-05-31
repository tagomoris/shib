var config = {
  hiveserver: {
    host: 'localhost',
    port: 10000
  },
  kyototycoon: {
    host: 'localhost',
    port: 1978
  }
};

var querystring = "SELECT hhmm, (split(fullpath,'\/'))[1] AS blogname, count(userlabel) AS cnt " +
  "FROM access_log WHERE service='blog' AND yyyymmdd='20110531' " +
  "GROUP BY hhmm, blogname SORT BY cnt DESC limit 100";

var shib = require('shib');
shib.init(config);

var qid;
console.log("====== execute stage ======");
shib.client().createQuery(querystring, [], function(err, query){
  console.log("queryid:" + query.queryid);
  qid = query.queryid;
  console.log(query);
  this.execute(query);
});

setTimeout(function(){
  console.log("====== display stage ======");
  console.log("queryid:" + qid);
  shib.client().getQuery(qid, function(err, query){
    console.log(query);
    this.status(query, function(status){
      console.log("status:" + status);
    });
    this.getLastResult(query, function(err, result){
      console.log(result);
      this.rawResultData(result.resultid, function(err, data){
        console.log("====== display stage ======");
        console.log(data);
      });
    });
  });
}, 3000);
