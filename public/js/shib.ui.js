$(function(){
  $('form.queryeditor').submit(function(e){execute_query(e);});
});

function execute_query(event){
  /* show waiting icon */

  $(event.target).ajaxSubmit({
    success: function(data){
      $('div#queryid').text(data);
    },
    error: function(xhr){
      $('div#queryid').text('error on execute...');
    }
  });
  event.preventDefault();

  /* setInterval check_query_status */
  
  return false;
};
