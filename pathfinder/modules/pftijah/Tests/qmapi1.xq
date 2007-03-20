let $opt:=<TijahOptions collection="testcoll1" txtmodel_model="NLLR" debug="0"/>
return tijah:query($opt,(),"//panel[about(.,speed)]")
