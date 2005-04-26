declare variable $a := 42;
declare function foo($x) { ($a, $b) };
declare variable $b := $a + 1;

($a, $b)
