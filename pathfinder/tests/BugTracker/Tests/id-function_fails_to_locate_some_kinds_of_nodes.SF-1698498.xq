let $doc := doc("id-function_fails_to_locate_some_kinds_of_nodes.SF-1698498.xml")
,   $a := (<a/>,<b/>,<c><d/></c>,$doc/descendant::node())
,   $nids := ( for $i in $a return pf:nid($i) )
for $ref at $p in $nids
let $o := id($ref,exactly-one($a[$p]))
return
   <r><nid>{$ref}</nid>
      <ori>{$a[$p]}</ori>
      <obj>{$o}</obj>
   </r>
