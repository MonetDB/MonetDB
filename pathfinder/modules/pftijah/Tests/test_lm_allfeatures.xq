let $opt := <TijahOptions 
                ft-index="thesis" 
                ir-model="LM" 
                debug="0"/>

let $query := "//chapter[about(.//title,information) and about(.//title,retrieval)]//section[about(.,XML) or about(.,SGML)]"

for $n at $r in tijah:queryall($query,$opt)
return <node rank="{$r}">{$n}</node>
