declare namespace local = "http://www.foobar.org";

declare variable $multiplier := <multiply>3</multiply>;

declare function local:times ($x as xs:integer) as xs:integer*
{
$multiplier cast as xs:integer * $x
};

declare function local:multi_times ($x as xs:integer*) as xs:integer*
{
for $y in $x return local:times ($y)
};

declare function local:match ($x as xs:integer*) as node()
{
let $a := (1,2,3)
return let $b := for $c in $x
                 where $c = ($a, local:multi_times($a))
                 return $c
       return element matching-numbers { $b }
};

local:match ((1,2,3,4,5,6,7,8,9))
