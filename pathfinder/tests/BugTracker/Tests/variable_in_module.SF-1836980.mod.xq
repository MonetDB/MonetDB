module namespace test = "http://www.cs.utwente.nl/~keulen";

declare variable $test:special := <special/>;

declare function test:specialpos($test as element()*)
   as xs:integer*
{
   for $e at $p in $test
   where $e is $test:special
   return $p
};
