let $a := doc("foo.xml")/a/*
return let $b := doc("foo.xml")//*
return
(
$a union $b
, 100,
$b union $a
, 100,
() union ()
, 100,
$a union ()
, 100,
() union $a
)
