let $x := <foo> <bar/> </foo> return
  <a xmlns='http://create.another/default/element/namespace'>
    { $x/*:bar }
  </a>
