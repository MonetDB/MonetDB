let $a := (1,2,3,4,5) return let $b := (10,20,30) return for $c in $a return let $a := (6,7) return for $d in (1,2) return ($a,$b,$d)
