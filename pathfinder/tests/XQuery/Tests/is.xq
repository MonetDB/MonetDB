let $a := exactly-one(doc("foo.xml")//d) return for $b in (doc("foo.xml")//node(), doc("book.xml")//book[3]) return if ($b is $a) then $b else "false"
