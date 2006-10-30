declare function local:between($seq as node()*, $start as node(), $end as node())
 as item()*
{
  let $nodes :=
    for $n in $seq except $start//node()
    where $n >> $start and $n << $end
    return $n
  return $nodes except $nodes//node()
};

<critical_sequence>
 {
  let $proc := doc("report1.xml")//section[section.title="Procedure"][1],
      $first :=  exactly-one(($proc//incision)[1]),
      $second:=  exactly-one(($proc//incision)[2])
  return local:between($proc//node(), $first, $second)
 }
</critical_sequence>
