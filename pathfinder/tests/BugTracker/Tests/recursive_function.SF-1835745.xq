declare function test($nodes as node()*) {
for $n in $nodes[./self::a]
let $c := test($n/*)
where (count($c) > 0)
order by $n/@name
return element a { $n/@name, $c } ,
for $n in $nodes[./self::b] return $n
}; 
test(<x><a name="1"></a><a name="2"><a name="y"><b name="2"/></a></a></x>/*) ,
test(<x><a name="1"><a name="x"/></a><a name="2"><a name="y"><b name="2"/></a></a></x>/*)
<>
declare function test_no_order($nodes as node()*) {
for $n in $nodes[./self::a]
let $c := test_no_order($n/*)
where (count($c) > 0)
return element a { $n/@name, $c } ,
for $n in $nodes[./self::b] return $n
}; 
test_no_order(<x><a name="1"></a><a name="2"><a name="y"><b name="2"/></a></a></x>/*) ,
test_no_order(<x><a name="1"><a name="x"/></a><a name="2"><a name="y"><b name="2"/></a></a></x>/*)
