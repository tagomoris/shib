$(function(){
  $('form.queryeditor').submit(function(e){execute_query(e);});
});

function execute_query(event){
  $(event.target).ajaxSubmit({
    success: function(){},
    error: function(){}
  });
  event.preventDefault();
  return false;
};
