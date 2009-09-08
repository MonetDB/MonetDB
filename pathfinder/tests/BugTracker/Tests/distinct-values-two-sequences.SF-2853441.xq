let $x := ("aap","beer")
let $y := <a><b>beer</b><b>noot</b></a>//b
for $i in distinct-values(($x,$y)) return $i
<>
let $x := ("aap","beer")
let $y := ("beer","noot")
for $i in distinct-values(($x,$y)) return $i
