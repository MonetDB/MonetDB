(: It is the URI that is actually of interest for the namespace
   part.  If we bind two prefixes to the same URI, they denote
   the same namespace. :)

declare namespace foo = "http://www.foo.bar/";
declare namespace bar = "http://www.foo.bar/";

(: use prefix foo to define variable... :)
declare variable $foo:v1 := 42;

(: ...and prefix bar to look it up. (which should be fine, of course) :)
$bar:v1
