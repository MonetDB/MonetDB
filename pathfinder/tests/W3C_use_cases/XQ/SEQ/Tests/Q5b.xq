<critical_sequence>
 {
  let $proc := doc("report1.xml")//section[section.title="Procedure"][1],
      $i1 :=  exactly-one(($proc//incision)[1]),
      $i2 :=  exactly-one(($proc//incision)[2])
  for $n in $proc//node() except $i1//node()
  where $n >> $i1 and $n << $i2
  return $n 
 }
</critical_sequence>
