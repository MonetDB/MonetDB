module namespace func = "xrpcwrapper-testfunctions";

declare function func:echoVoid()
{ () };

declare function func:echoInteger($v as xs:integer*) as xs:integer
{ exactly-one($v) };

declare function func:echoDouble($v as xs:double*) as xs:double
{ exactly-one($v) };

declare function func:echoString($v as xs:string*) as xs:string
{ exactly-one($v) };

declare function func:echoParam($v1 as xs:double*, $v2 as item()*) as
item()*
{
    ($v1, $v2)
};

declare function func:getPerson($personDoc as xs:string, $pid as xs:string)
as node()*
{ for $x in doc($personDoc)//person
  where $x/@id = $pid
    return $x
};

declare function func:getDoc($doc as xs:string) as document-node()*
{ doc($doc) };

declare function func:firstClosedAuction($auctionDoc as xs:string) as node()*
{ doc($auctionDoc)//closed_auction[1] };

declare function func:buyerAndAuction($personDoc as xs:string, $auctionDoc as xs:string) as node()*
{
  for $p in doc($personDoc)//person,
      $ca in doc($auctionDoc)//closed_auction
  where $p/@id = $ca/buyer/@person
  return <buyer-auction>{$p, $ca}</buyer-auction>
};

declare function func:auctionsOfBuyer($auctionDoc as xs:string, $pid as xs:string) as node()*
{
  for $ca in doc($auctionDoc)//closed_auction
  where $ca/buyer/@person = $pid
  return $ca
};
