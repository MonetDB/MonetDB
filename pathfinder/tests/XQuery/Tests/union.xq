let $a := doc("foo.xml")/a/*
return let $b := doc("foo.xml")//*
return for $c in (doc("book.xml")//author)[1]
return ($a, $c) union $b

