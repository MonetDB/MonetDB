declare function foo ($x as xs:integer) as xs:integer
{
    bar ($x)
};

declare function bar ($x as xs:integer) as xs:integer
{
    $x + 3
};

foo (3)
