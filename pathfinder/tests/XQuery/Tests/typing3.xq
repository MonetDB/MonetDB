(: The `as xs:decimal' sets the *static* type of $a to
   xs:decimal (without affecting its *dynamic type).
   Function foo() is thus called with ill-typed parameters,
   and static typing will complain. :)
declare function foo ($x as xs:integer)
{
  $x
};

let $a as xs:decimal := 42 return foo ($a)
