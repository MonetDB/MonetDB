let $a := <a b="10"/> 
for $b in $a 
return element { "d" } { $b }
