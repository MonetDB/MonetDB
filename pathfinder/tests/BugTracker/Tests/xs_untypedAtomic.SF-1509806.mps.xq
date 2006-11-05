declare function foo ($a as xs:string, $b as xs:string)
{
fn:concat ($a, $b)
};

for $a in (<a/>, "foo") return foo ($a, "bar")
