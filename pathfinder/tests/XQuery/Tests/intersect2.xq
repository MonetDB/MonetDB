let $a := doc("foo.xml")/a/*
return let $b := doc("foo.xml")//*
return
(
$a intersect $b
, 100,
$b intersect $a
, 100,
() intersect ()
, 100,
$a intersect ()
, 100,
() intersect $a
)
