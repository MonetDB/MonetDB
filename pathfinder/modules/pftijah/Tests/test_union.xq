let $opt := <TijahOptions 
                ft-index="thesis" 
                ir-model="LMS" 
                return-all="false"
                debug="0"/>

let $query := "//(chapter|section)//para[about(.,information retrieval)]"

for $n at $r in tijah:queryall($query,$opt)
return <node rank="{$r}">{$n}</node>
