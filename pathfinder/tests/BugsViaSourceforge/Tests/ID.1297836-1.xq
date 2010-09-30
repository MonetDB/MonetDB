let $d := <file><file>a</file><file>b</file></file>
return
if (1 = 0) then
()
else
let $f := $d//file
return element { "result" } {
element { "matches" } { count($f) },
element { "results" } {
for $i in 1 to 10
return element { "result" } { count($f[$i]/*) }
}
} 

