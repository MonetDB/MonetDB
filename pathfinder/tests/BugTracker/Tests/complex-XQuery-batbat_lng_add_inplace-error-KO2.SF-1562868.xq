declare function delete($elems as xs:integer*, $elem as xs:integer*)
   as xs:integer*
{
   for $e in $elems
   where not($e=$elem)
   return $e
};

declare function seqIntCand($xlist as xs:integer*, $ylist as xs:integer*)
   as element(list)*
{
   if (count($xlist) eq 0)
   then <list>{for $y in $ylist return <cand a="{$y}"/>}</list>
   else if (count($ylist) eq 0)
   then <list>{for $x in $xlist return <cand a="{$x}"/>}</list>
   else
      let $x := exactly-one($xlist[1])
         ,$xs := $xlist[position()>1]
      return
        (seqIntCand2($x,$xs,$ylist)
        ,for $l in seqIntCand($xs,delete($ylist,$x))
         return <list>{<cand a="{$x}"/>,$l/cand}</list>
        )
};

declare function seqIntCand2($x as xs:integer
                            ,$xs as xs:integer*
                            ,$ylist as xs:integer*)
   as element(list)*
{
   for $y in $ylist
   return
      let $ysplusydone := delete($ylist,$y)
         ,$pairrest := seqIntCand(delete($xs,$y),delete($ysplusydone,$x))
      return
         if ($x eq $y)
         then ()
         else for $l in $pairrest
              return <list>{<cand a="{$x}" b="{$y}"/>,$l/cand}</list>
};

declare function constructPossibility($all as node()*,$l as element(list))
   as element()*
{
   for $cand in $l/cand
   return
      if (count($cand/@b) gt 0)
      then let $e1:=exactly-one($all[number ($cand/@a)])
           return integrate_e($e1,$e1)
      else <prob></prob>
};

declare function integrate_e($pta as node(),$ptb as node())
   as element()*
{
   let $ptsa := $pta/child::node()
      ,$ptsb := $ptb/child::node()
      ,$all := ($ptsa,$ptsb)
      ,$ptsacode:=(1 to count($ptsa))
      ,$ptsbcode:=(count($ptsa)+1 to count($all))
   return
      for $l in seqIntCand($ptsacode,$ptsbcode)
      return constructPossibility($all,$l)
};

(: TEST :)

let $doc1 := <movie><title>King Kong</title><year>1933</year></movie>
   ,$doc2 := <movie><title>King Kong</title><year>1976</year></movie>
return
   integrate_e($doc1,$doc2) 
