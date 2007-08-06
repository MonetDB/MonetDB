declare function int-check($ylist as xs:integer*) { $ylist };
int-check(distinct-values(1 to 3))
