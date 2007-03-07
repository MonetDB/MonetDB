module namespace foo = "xrpc-test-function";

declare function foo:add($v1 as xs:integer,
                         $v2 as xs:integer) as xs:integer
{
    $v1 + $v2
};
