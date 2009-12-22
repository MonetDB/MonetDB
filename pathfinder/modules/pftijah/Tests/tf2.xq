let $d := pf:collection("thesis.xml")
let $opt := <TijahOptions ft-index="snowball"/>
let $s := "XML"
for $e in $d//section
return tijah:tf($e, $s, $opt)
