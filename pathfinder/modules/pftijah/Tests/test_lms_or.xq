let $opt := <TijahOptions 
                collection="thesis" 
                txtmodel_model="LMS" 
                txtmodel_returnall="false"
                debug="0"/>

return pf:tijah-query($opt,(),"//title[about(.,pathfinder) or about(.,tijah)]")
