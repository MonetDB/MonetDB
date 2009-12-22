let $d := pf:collection("thesis.xml")
let $opt := <TijahOptions ft-index="snowball"/>
let $e := $d//section
for $s in ("XML","Tijah")
return tijah:tf($e, $s, $opt)
