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

declare function dfx:auctionsAllPerson(
			$dst as xs:string,
			$doc as xs:string) as node()*
{ 
	for $pid in subsequence(doc($doc)//person/@id,0,5)
	return 	
		execute at {$dst} {f2:auctionsByPerson($doc, 'person10')}
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
  do delete doc($doc)//person[./@id=$pid]
};

declare updating function dfx:deletePersonNested(
			$dsts as xs:string+,
			$doc as xs:string,
			$pid as xs:string)
{
  (do delete doc($doc)//person[./@id=$pid],
   for $dst in $dsts return
	  execute at {$dst} {dfx:deletePerson($doc, $pid)})
};

declare function dfx:repeatable($dst as xs:string) as node()*
{
	let $d1 := execute at {$dst} {dfx:getdoc("hello.xml")},
	    $slow := count(for $i in 1 to 10 return element bla {doc("xmark2.xml")}),
	    $d2 := execute at {$dst} {dfx:getdoc("hello.xml")}
	return ($d1//hello, $d2//hello, <slow>{$slow}</slow>)
};

declare function dfx:getdoc($url as xs:string) as document-node()?
{ doc($url) };

