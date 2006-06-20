declare function output-elt ($elt as xs:string, $node
as node()) as node() {
element { $elt } { $node/@* }
};

for $np in doc("foo.xml")/a,
$name in $np/c
return
    <entry>
    { output-elt("np", $np) }
    { output-elt("name", $name) }
    </entry>
