let $opt:=<TijahOptions milfile="/tmp/x" collection="testcoll1" txtmodel_model="NLLR" debug="0"/>
let $x := pf:tijah-query-id($opt,(),"//panel[about(.,speed)]")
let $n := pf:tijah-nodes($x)
for $i in $n return (fn:round(100*pf:tijah-score($x,$i)),$i)
