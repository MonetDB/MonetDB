module namespace f = "foo";

declare function f:comparenodes(
        $used as node()*, $returned as node()*, $referred as node()*,
        $upath as xs:string*, $rpath as xs:string*,
        $u as node()*, $r as node()* ) as node()*
{ f:comparenodes($u, $r) };

declare function f:comparenodes($u as node()*, $r as node()*) as node()*
{
    for $i in $u, $j in $r
    where $i << $j
    return $i
};

