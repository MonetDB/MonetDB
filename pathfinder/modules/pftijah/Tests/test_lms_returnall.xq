let $opt := <TijahOptions 
                ft-index="thesis" 
                ir-model="LMS" 
                return-all="true"
                debug="0"/>

let $query := "//section[about(.,pathfinder)]"

for $n at $r in tijah:queryall($query,$opt)
return <node rank="{$r}">{$n}</node>
