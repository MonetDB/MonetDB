let $opt := <TijahOptions 
                collection="thesis" 
                txtmodel_model="LMS" 
                txtmodel_returnall="true"
                debug="0"/>

let $query := "//section[about(.,pathfinder)]"

for $n at $r in pf:tijah-query($opt,(),$query)
return <node rank="{$r}">{$n}</node>
