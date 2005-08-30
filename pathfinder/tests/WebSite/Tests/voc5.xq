  for $t in doc("voc.xml")//voyage
  let $p := zero-or-one($t/rightpage/particulars/text())
  where not($t//destination/arrival)
          and (contains($p,"wrecked")
          or contains($p,"sunk"))
  order by $t/leftpage/boatname/text()
  return 
    element { "boat" } { 
      element { "name" } { $t/leftpage/boatname/text() }, 
      text { ": " },
      element { "description" } { $t/rightpage/particulars/text() } 
    } 
