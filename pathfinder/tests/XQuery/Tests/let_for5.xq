for $a in (1,2) return 
	(let $b := ($a,3,4) return
	 for $c in (8,9)
		 return ($b,$c),
         let $b := (6,7) return ($b,$a,$b))
