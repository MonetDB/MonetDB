let $a := exactly-one(doc("foo.xml")/a) return let $d := element d { text {""}} return $a << $d
