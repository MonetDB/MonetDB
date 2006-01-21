let $a := (<a/>,<a/>)
for $i in 1 to 2
let $b := zero-or-one($a[$i])
return element { name($b) } { $b/@* }
