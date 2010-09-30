let $x := (2,3)
let $y := (10,20)[fn:position() < 10]
for $i at $rank in $y
return <item> {$rank} - {$i} - {$x[$rank]} </item>
