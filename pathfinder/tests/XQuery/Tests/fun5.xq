declare namespace local = "http://www.foobar.org";

declare variable $ints := (1,2,3);
declare variable $multiplier := <multiply>2</multiply>;

declare function local:times ($x as xs:integer) as xs:integer*
{
$multiplier cast as xs:integer * $x
};

declare function local:multi_times ($x as xs:integer*) as xs:integer*
{
for $y in $x return local:times ($y)
};

declare function local:built_res ($x as xs:integer *) as node()*
{
for $y in $strings
return element { $y } { $x }
};
declare variable $strings := ("foo", "bar");

local:built_res(local:multi_times ($ints))

