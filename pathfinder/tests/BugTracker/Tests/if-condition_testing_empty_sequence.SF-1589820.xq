declare function depthKO($x as node()) as xs:integer
{
let $m := max(for $c in $x/child::node() return
depthKO($c))
return
if ($m) then 1 + exactly-one ($m) else 1
};

declare function depthOK($x as node()) as xs:integer
{
let $m := max(for $c in $x/child::node() return
depthOK($c))
return
if (count($m) gt 0) then 1 + exactly-one ($m) else 1
};

(
depthKO(<a><b/><b><c/></b></a>)
,
depthOK(<a><b/><b><c/></b></a>)
)
