let $opt := <TijahOptions 
                ft-index="thesis" 
                ir-model="LMS" 
		rmoverlap="true"
                debug="0"/>

let $query := "//*[about(.,pathfinder)]"

for $n at $r in tijah:queryall($query,$opt)
return <node rank="{$r}">{$n}</node>
