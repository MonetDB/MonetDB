let $opt := <TijahOptions debug="0" ft-index="FT_PFCOLL" ir-model="NLLR"/>
let $coll := fn:collection("PFCOLL")
let $qtext := "//bubble[about(., Speed Snelheid)]"
return tijah:query($coll,$qtext, $opt)
