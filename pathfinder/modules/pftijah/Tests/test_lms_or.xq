let $opt := <TijahOptions 
                ft-index="thesis" 
                ir-model="LMS" 
                txtmodel_returnall="false"
                debug="0"/>

let $query := "//title[about(.,pathfinder) or about(.,tijah)]"

for $n at $r in tijah:query($query,$opt)
return <node rank="{$r}">{$n}</node>
