let $a := exactly-one(doc("foo.xml")//d) return 
for $b in (doc("foo.xml")//b, doc("book.xml")//book[3],doc("foo.xml")//c, <a/>) 
return if ($a >> $b) then $b else "false"
