let $a := (1,2) return
for $b in $a return
	let $e := (1,3) return
	(for $c in ($b, 1) return
		($e,$b,$e), 1000, for $d in (3,4) return
					for $f in (5,6) return
						($a,$a,$d,$f),
		9999)
