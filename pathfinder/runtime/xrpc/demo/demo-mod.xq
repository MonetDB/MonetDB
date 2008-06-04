module namespace f="xrpcdemo-functions";

declare function f:add($v1 as xs:integer, $v2 as xs:integer) as xs:integer
{ $v1 + $v2 };

declare function f:firstPerson($doc as xs:string) as node()
{ exactly-one(doc($doc)//person[1]) };

declare function f:firstPerson($docs as xs:string+, $dsts as xs:string+) as node()+
{ for $d at $pos in $dsts
  return execute at {$d} {f:firstPerson(exactly-one($docs[$pos]))}
};

declare function f:boughtItems($pid as xs:string, $doc as xs:string) as node()*
{ doc($doc)//closed_auction[./buyer/@person=$pid] };

declare function f:boughtItemsAllPersons($docL as xs:string, $docR as xs:string, $dst as xs:string) as node()*
{ for $pid in doc($docL)//person/@pid
  return execute at {$dst} {f:boughtItems($pid, $docR)}
};

declare updating function f:insertPerson($p as node(), $doc as xs:string)
{ do insert $p into doc($doc)//persons };

declare updating function f:deletePerson($pid as xs:string, $doc as xs:string)
{ do delete doc($doc)//person[./@pid=$pid] };

