module namespace fun = "bugtracker-function";

declare function fun:add($v1 as xs:integer, $v2 as xs:integer) as xs:integer
{
    $v1 + $v2
};


declare function fun:add($dst as xs:string, $v1 as xs:integer, $v2 as xs:integer) as xs:integer
{
    execute at {$dst} {fun:add($v1, $v2)}
};
