let $a := doc("book.xml")//book[2]/author[1] return $a/descendant-or-self::node()
