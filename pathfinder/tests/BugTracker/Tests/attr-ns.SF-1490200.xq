declare namespace a = "foo"; 

let $z := <z><a xmlns="foo" at3="foo"/></z>
return 
  (<a>{ $z//@a:* }</a>,
   <b>{ $z//@at3 }</b>)
