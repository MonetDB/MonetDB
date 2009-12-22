let $d := pf:collection("thesis.xml")
let $opt := <TijahOptions ft-index="snowball"/>
let $e := $d//thesis
for $s in ("XML","Tijah","Pathfinder")
return (tijah:tf-all($s, $opt), tijah:tf($e, $s, $opt))
