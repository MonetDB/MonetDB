let $a := doc("foo.xml")/a return for $b in ($a, $a) return $b/*[last()]
