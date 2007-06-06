let $node := <a><b><c/></b><d><e/></d></a>
for $desc in $node/descendant::*
return count($desc/../preceding::*)
