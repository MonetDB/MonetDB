declare function foo () as xs:string
{
    "foo"
};
declare function bar ($x as xs:integer) as xs:string
{
    "foo"
};
foo(), bar(42)
