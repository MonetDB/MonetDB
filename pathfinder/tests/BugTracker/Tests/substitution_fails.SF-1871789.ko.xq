element { "a" }{
for $a in doc("x.xml")//a
let $m := distinct-values($a//d)
for $q in distinct-values($a//c[d])
for $r in $m
where some $x in $a//c satisfies $x[e/d[contains(.,$r)] and a=$q]
return element { "a" } { $a//c }
}
