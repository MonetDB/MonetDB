let $f := doc("HelloWorld.xml")/doc
for $g in $f/greet
for $l in $f/location
return
<sentence> {
$g/text(),$l/text()
} </sentence>
