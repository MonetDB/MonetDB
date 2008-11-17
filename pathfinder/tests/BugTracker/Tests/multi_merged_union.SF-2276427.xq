let $notesdoc := <root>
                     <objectnote xid="3"/>
                     <objectnote xid="1"/>
                     <objectnote xid="1"/>
                 </root>
for $i in (<link xid="3"/>, <link xid="1"/>)
let $note := ($notesdoc/objectnote[@xid=$i/@xid])[1]
return element { name($i) } { $i/@xid, $note }
