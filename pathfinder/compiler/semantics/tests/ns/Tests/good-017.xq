(: We're interested in namespace resolution in this test case.
   But note that effects will not be visible before variable
   scoping.  This is different to most other test cases in this
   directory. :)
declare namespace a = "foo";
declare namespace b = "foo";

let $a:foo := 42 return $b:foo + 1
