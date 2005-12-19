let $a := (<a/>,<a/>)
for $i in 0 to 3
let $b := zero-or-one($a[$i])
return element { name($b) } { $b/@* }
