element { "a" }{
for $a in doc("x.xml")//a
for $q in distinct-values($a//c[d])
for $r in distinct-values($a//d)
where some $x in $a//c satisfies $x[e/d[contains(.,$r)] and a=$q]
return element { "a" } { $a//c }
}
