declare function bar ($x as xs:integer) as xs:integer
{
    $x + 3
};

declare function foo ($x as xs:integer) as xs:integer
{
    bar ($x)
};

foo (3)
