<result> {
let $entries := <aap/>//entry
for $i in distinct-values($entries//docs)
return
<cluster> {
element { "name" } { $i },
let $items := $entries[docs=$i]
let $exts := for $a in $items/names/name
return substring-after($a,"a")
for $j in $exts
return <hit> {
$items/names/name[ends-with(.,$j)]
} </hit>
} </cluster>
} </result>
