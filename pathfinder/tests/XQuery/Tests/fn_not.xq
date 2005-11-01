(: select all elements in our mini-document that do NOT
   have any children (node d only) :)
let $doc := <a>
              <b>foo</b>
              <c><foo/></c>
              <d/>
            </a>
  return
    $doc/descendant::*[not(child::node())]
