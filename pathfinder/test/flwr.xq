for $a in 1 return 2
--
for $a in (1,2,3) return 4
--
for $a in (1,2), $b in (3,4), $c in (5,6) return 7
--
for $a in (1,2) let $b := (3,4) for $c in (5,6) return ($a,$b,$c)
--
for $a in (1,2) where 3 < 4 return 5
--
for $a in (1,2), $b in ($a, 2) return $b
--
let $a := 1 return $a + 1
--
let $a := 2 where 3 < 4 return $a
--
let $a := 1 for $b in (1,2) return $a + $b
--
let $a := 1, $b := 2 return $a + $b
--
let $a := (1,2) for $b in $a return $b
--
let $a := 1 for $b in ($a, 1) return $b + $a
--
for $a in (1,2,3) 
where $a > 2 
order by $a - 2 descending empty greatest collation "foo"
return $a - 2
