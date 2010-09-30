let $f := doc("HelloWorld.xml")/doc
for $g in $f/greet/text()
for $l in $f/location/text()
return
<sentence> {
$g,$l
} </sentence>
