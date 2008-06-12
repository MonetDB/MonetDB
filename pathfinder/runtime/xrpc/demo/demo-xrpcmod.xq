module namespace dfx="demo-functions-xrpc";

import module namespace f2 = "demo-functions"
		at "http://localhost:50001/demo/demo-mod.xq";

declare function dfx:getPerson(
          $doc as xs:string,
          $pid as xs:string) as node()*
{
	for $p in doc($doc)//person[./@id = $pid]
	return
		element person {
			attribute id {$p/@id},
  			$p/name }
};

(:
declare function dfx:auctionsByPerson(
          $doc as xs:string,
          $pid as xs:string) as node()*
{
  for $ca in doc($doc)//closed_auction[./buyer/@person=$pid]
  return
    element closed_auction {
      attribute buyer {$ca/buyer/@person},
      $ca/price
    }
};
:)

declare function dfx:auctionsAllPerson(
			$dst as xs:string,
			$doc as xs:string) as node()*
{ 
	for $pid in doc($doc)//person/@id
	return 	
		execute at {$dst} {f2:auctionsByPerson($doc, $pid)}
};

declare function dfx:nestedRPC(
          $dst1 as xs:string,
          $dst2 as xs:string,
          $dst3 as xs:string,
          $doc as xs:string) as node()*
{
  (execute at {$dst1} {dfx:auctionsAllPerson($dst3, $doc)},
   execute at {$dst2} {dfx:auctionsAllPerson($dst3, $doc)})
};

declare updating function dfx:insertPerson(
          $doc as xs:string,
          $p as node())
{
  do insert $p into doc($doc)//people
};

declare updating function dfx:insertPersonNested(
			$dsts as xs:string+,
			$doc as xs:string,
			$p as node())
{
  (do insert $p into doc($doc)//people,
   for $dst in $dsts return
	  execute at {$dst} {dfx:insertPerson($doc, $p)})
};

declare updating function dfx:deletePerson(
          $doc as xs:string,
          $pid as xs:string)
{
  do delete doc($doc)//person[./@pid=$pid]
};

declare updating function dfx:deletePersonNested(
			$dsts as xs:string+,
			$doc as xs:string,
			$pid as xs:string)
{
  (do delete doc($doc)//person[./@pid=$pid],
   for $dst in $dsts return
	  execute at {$dst} {dfx:deletePerson($doc, $pid)})
};

declare function dfx:getdoc($url as xs:string) as document-node()?
{ doc($url) };

