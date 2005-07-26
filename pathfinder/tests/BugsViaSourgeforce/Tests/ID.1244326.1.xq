declare function blob($n as node()) as attribute()*
{
  let $x := $n/ancestor::container[@blob] except $n/ancestor::container[@blob]/ancestor::*
  return  
    if ($x[fn:last()]) then 
      ($x/@blob, $x/@xoffset)
    else
      ()
   (: 
     if ($x[fn:last()]/@) then 
      ($x[fn:last()],$
    else
      attribute { "blob" } { "-" }  :)
};
declare function select_narrow($context as node()*) as node()* {
  for $t in $context
  return xdoc()//*[@start >= $t/@start 
              and @end <= $t/@end] 
  except $t/ancestor::*
};
declare function select_wide($context as node()*) as node()* {
  for $t in $context
  return xdoc()//*[@start >= $t/@start and @start <= $t/@end
              or @end >= $t/@start and @end <= $t/@end] 
         except $t/ancestor::*
};


declare function xdoc() as node()? { doc("ID.1244326.xml") };
 let $a := xdoc()//file
 let $b := "aap"
return element { "XIRAF" } {
  attribute { "tool" } { $b },
  for $t in ($a)
  where $t/@xstart and $t/@xend and $t/@xid
    and not($t/container/@tool = $b)    
  return element { "item" } {
         $t/@xstart,
	 $t/@xend,
	 $t/@xid,
	 blob($t)
  },
  for $t in ($a)
  where not($t/@xstart) and not($t/@xend) and $t/@xid
         and not($t/container/@tool = $b)    
  return element { "item" } {
    $t/@xid,
    for $s in $t/*
    where $s/@xstart and $s/@xend and $s/@xid    
      return element { "region" } {
         $t/@xstart,
	 $t/@xend,
	 $t/@xid,
	 blob($t)
      }
  }
}
