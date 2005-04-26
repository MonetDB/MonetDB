declare variable $a := 42;
declare variable $b := $a + 1;

for $i in ($a, $b) return ($i, $a, $b)
