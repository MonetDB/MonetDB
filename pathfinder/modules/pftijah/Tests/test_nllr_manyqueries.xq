let $opt := <TijahOptions 
                ft-index="thesis" 
                ir-model="LMS" 
                txtmodel_returnall="false"
                debug="0"/>

let $query := "//chapter[about(.//title,information) and about(.//title,retrieval)]//section[about(.,pathfinder) or about(.,tijah)]"

for $i in (0 to 1000)
    for $n at $r in tijah:queryall($query,$opt)
    return <node rank="{$r}">{$n}</node>
