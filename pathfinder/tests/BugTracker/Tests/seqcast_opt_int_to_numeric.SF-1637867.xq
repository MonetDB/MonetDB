let $a := (<a num="1"/>, <b num="12"/>) return
$a[./@num cast as xs:integer?]

(: expected result is '<a num="1"/>' :)
