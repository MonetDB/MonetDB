let $r := <root><a/><b/><c><d/></c></root>
let $a := $r/*, $nids := ( for $i in $a return pf:nid($i) )
for $o in id($nids,exactly-one($r))
return <r>{$o}<eq>{$o is exactly-one($a[1])}</eq></r>
