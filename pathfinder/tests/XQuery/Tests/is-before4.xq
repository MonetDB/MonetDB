let $a := exactly-one(doc("foo.xml")//d) return for $b in (doc("foo.xml")//node(), doc("book.xml")//book[3]) return if ($a << $b) then $b else "false"
