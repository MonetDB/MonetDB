(: URI for namespace a equals the default element namespace :)
declare namespace a = "http://asdf.com/example1";
declare namespace b = "http://asdf.com/example2";

let $x := (<a:foo> <a:bar/> </a:foo>, <b:foo> <b:bar/> </b:foo>)
  return
    $x/bar
