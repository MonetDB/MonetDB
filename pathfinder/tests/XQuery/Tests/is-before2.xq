let $a := exactly-one(doc("foo.xml")/a) return let $d := exactly-one(doc("foo.xml")//d) return $d << $a
