let $a := (1,2,3) return (let $a := (1,2) return $a, for $b in (4,5,$a) return $b)
