let $a := doc("foo.xml")/a/*
return let $b := doc("foo.xml")//*
return
(
$a except $b
, 100,
$b except $a
, 100,
() except ()
, 100,
$a except ()
, 100,
() except $a
)
