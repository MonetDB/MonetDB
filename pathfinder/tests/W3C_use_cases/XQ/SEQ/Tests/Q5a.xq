declare function local:precedes($a as node(), $b as node()) as xs:boolean 
{
    $a << $b
      and
    empty($a//node() intersect $b)
};


declare function local:follows($a as node(), $b as node()) as xs:boolean 
{
    $a >> $b
      and
    empty($b//node() intersect $a)
};

<critical_sequence>
 {
  let $proc := doc("report1.xml")//section[section.title="Procedure"][1]
  for $n in $proc//node()
  where local:follows($n, ($proc//incision)[1])
    and local:precedes($n, ($proc//incision)[2])
  return $n
 }
</critical_sequence>
