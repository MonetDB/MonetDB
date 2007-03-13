let $a := (<a/>,<b/>,<c><d/></c>)
, $nids := ( for $i in $a return pf:nid($i) )
for $o in id($nids,<a/>)
return <r>{$o}<eq>{$o is exactly-one($a[1])}</eq></r>
