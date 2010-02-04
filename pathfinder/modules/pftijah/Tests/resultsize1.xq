let $opt := <TijahOptions returnNumber="2" ft-index="snowball"/>
let $query := "//section[about(., retrieval)]"
let $resID := tijah:queryall-id($query,$opt)
return <results num="{tijah:resultsize($resID)}">
{
   for $n at $r in tijah:nodes($resID)
   return <node rank="{$r}">{$n}</node>
}
</results>
