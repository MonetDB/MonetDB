let $f := doc("http://monetdb.cwi.nl/XQuery/files/HelloWorld.xml")/doc
for $g in $f/greet
for $l in $f/location
return 
  <sentence> { 
    $g/text(),$l/text()
  } </sentence>
