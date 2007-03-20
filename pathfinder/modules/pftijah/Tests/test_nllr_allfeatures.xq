let $opt := <TijahOptions 
                collection="thesis" 
                txtmodel_model="NLLR" 
                txtmodel_returnall="false"
                debug="0"/>

let $query := "//chapter[about(.//title,information) and about(.//title,retrieval)]//section[about(.,XML) or about(.,SGML)]"

for $n at $r in tijah:query($opt,(),$query)
return <node rank="{$r}">{$n}</node>
