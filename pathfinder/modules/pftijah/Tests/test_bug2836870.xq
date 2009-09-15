let $opt := <TijahOptions ft-index="thesis"/>
let $query := "//section[about(., 'XML information') or about(., 'XML database')]"
for $n at $r in tijah:queryall($query,$opt)
return <node rank="{$r}">{$n}</node>

