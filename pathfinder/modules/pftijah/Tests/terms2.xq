let $d := pf:collection("thesis.xml")
let $opt := <TijahOptions ft-index="snowball"/>
return tijah:terms($d//section, $opt)
