max(for $t in doc("ID.1244326.xml")//*/@xid 
return exactly-one($t) cast as xs:integer)
