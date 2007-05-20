let $d := doc("NULL_BAT_in_serializer.SF-1694814b.files.xml")
for $a in subsequence($d//file, 0, 1)
let $obj := element grr { $a/@* }
return element bug { $obj/@* }
