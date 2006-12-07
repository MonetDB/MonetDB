declare function recursiveActor($destinations as xs:string*, $actor as xs:string) as node()*
{
let $db := <film><filmName>x</filmName><actorName>Sean Connery</actorName></film>
let $val := $db//filmName[../actorName=$actor]
let $cnt := count($destinations)
let $pos := ($cnt div 2) cast as xs:integer
let $dsts1 := subsequence($destinations, 1, $pos)
let $dsts2 := subsequence($destinations, $pos+1)
return
(
if ($cnt > 1)
then recursiveActor($dsts1, $actor)
else ()
,
$val
,
if ($cnt > 2)
then recursiveActor($dsts2, $actor)
else ()
)
};

recursiveActor(("http://a.org","http://b.org","http://c.org","http://d.org","http://e.org"), "Sean Connery")
