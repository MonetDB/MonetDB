module namespace func = "xrpcwrapper-testfunctions";

declare function func:echoVoid()
{ () };

declare function func:echoInteger($v as xs:integer*) as xs:integer
{ exactly-one($v) };

declare function func:echoDouble($v as xs:double*) as xs:double
{ exactly-one($v) };

declare function func:echoString($v as xs:string*) as xs:string
{ exactly-one($v) };

declare function func:echoParam($v1 as xs:double*, $v2 as item()*) as node()
{
  element params {
    for $f in $v1
    return element double { $f },
    for $n in $v2
    return element node { $n }
  }
};

declare function func:getPerson($doc as xs:string, $pid as xs:string)
as node()*
{ for $x in doc("/tmp/persons.xml")//person
  where $x/@id = $pid
    return $x
};

declare function func:getDoc($doc as xs:string) as document-node()*
{ doc(fn:concat("/tmp/", $doc)) };

declare function func:Q_B1() as node()*
{ doc("/tmp/auctions.xml")//closed_auction[1] };

declare function func:Q_B2() as node()*
{
  for $p in doc("/tmp/persons.xml")//person,
      $ca in doc("/tmp/auctions.xml")//closed_auction
  where $p/@id = $ca/buyer/@person
  return <result>{$p, $ca/annotation}</result>
};

declare function func:Q_B3($pid as xs:string) as node()*
{
  for $ca in doc("/tmp/auctions.xml")//closed_auction
  where $ca/buyer/@person = $pid
  return $ca
};
