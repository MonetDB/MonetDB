declare namespace pxmlsup = "http://www.cs.utwente.nl/~keulen/pxmlsup";
let $wsd := (<pxmlsup:v1>1</pxmlsup:v1>,<pxmlsup:v2>3</pxmlsup:v2>)
let $wsds :=
   for $w in $wsd
   return string-join((string(node-name($w)),string($w)),"=")
return $wsds
<>
declare namespace pxmlsup = "http://www.cs.utwente.nl/~keulen/pxmlsup";
let $wsd := (<pxmlsup:v1>1</pxmlsup:v1>,<pxmlsup:v2>3</pxmlsup:v2>)
let $wsds :=
   for $w in $wsd
   return string-join((string(local-name($w)),string($w)),"=")
return $wsds
