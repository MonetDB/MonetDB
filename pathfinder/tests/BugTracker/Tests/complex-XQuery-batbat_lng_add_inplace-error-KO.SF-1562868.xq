declare function deep-equal($node1 as element(), $node2 as element())
   as xs:boolean
{
   let $c1 := $node1/descendant-or-self::node()
      ,$c2 := $node2/descendant-or-self::node()
   return
      if (count($c1) eq count($c2))
      then every $b in (for $c at $p in $c1
                        let $cc := exactly-one($c2[$p])
                        return $c/@name=$cc/@name)
           satisfies $b
      else false()
};

declare function index-of($all as element()*, $elems as element()*)
   as xs:integer*
{
   for $a at $p in $all
   where some $e in $elems satisfies $a is $e
   return $p
};

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

declare function probIntCand($all as element()*, $xlist as element(list)*)
   as element(list)*
{
   for $x in $xlist
   return
      if (candAgainstDTD($all,$x)) then () else <list prob="1">{$x/cand}</list>
};

declare function candAgainstDTD($all as element()*, $lpts as element(list))
   as xs:boolean
{
   false()
};

declare function theOracle_e($node1 as element(), $node2 as element())
   as xs:decimal
{
   let $e1:=exactly-one($node1/name())
       ,$e2:=exactly-one($node2/name())
       ,$names:=("name","persons","imdb","peggy","title","year","genres","directors")
   return
      if ($e1 eq $e2)
      then if ($e1=$names or deep-equal($node1,$node2)) then 1.0 else 0.5
      else 0.0
};

declare function constructIntPoss($all as element()*, $ptlist as element(list))
   as element()*
{
   for $pt in $ptlist/cand
   return
      if (count($pt/@b) gt 0)
      then integrate($all,exactly-one($all[number ($pt/@a)])
                         ,exactly-one($all[number ($pt/@b)]))
(:
      then <integrate><one>{exactly-one($all[number ($pt/@a)])}</one>
                      <two>{exactly-one($all[number ($pt/@b)])}</two></integrate>
:)
      else <prob><poss prob="1.0">{$all[number ($pt/@a)]}</poss></prob>
};

declare function integrate($all as element()*
                          ,$node1 as element()
                          ,$node2 as element())
   as element(prob)
{
   let $pta := $node1
      ,$ptb := $node2
      ,$ptsa := $pta/child::element()
      ,$ptsb := $ptb/child::element()
      ,$oracle := theOracle_e($pta,$ptb)
      ,$ea:=exactly-one($pta/name())
      ,$eb:=exactly-one($ptb/name())
      ,$ptsacode:=index-of($all,$ptsa)
      ,$ptsbcode:=index-of($all,$ptsb)
   return
      if (deep-equal($node1,$node2))
      then <prob><poss prob="1.0">{$node1}</poss></prob>
      else
      if ($oracle gt 0.1 and $ea eq $eb)
      then
         <prob>{
(:
         <debug>{probIntCand($all,seqIntCand($ptsacode,$ptsbcode))}</debug>,
:)
         for $l in probIntCand($all,seqIntCand($ptsacode,$ptsbcode))
         let $b:=exactly-one($l/@prob)
         where $b>0.1
         return <poss>{$b}
                  {element {$ea} {constructIntPoss($all,$l)}}
                </poss>
         }</prob>
      else <prob><poss prob="0.5">{$node1}</poss><poss prob="0.5">{$node2}</poss></prob>
};

(: TEST :)

let $da1 := <movie><title><king/></title><old/></movie>
   ,$da2 := <movie><title><king/></title><new/></movie>
   ,$alla := ($da1//descendant-or-self::element(),$da2//descendant-or-self::element())
   ,$db1 := <movie><title><king/><new/></title></movie>
   ,$db2 := <movie><title><king/><old/></title></movie>
   ,$allb := ($db1//descendant-or-self::element(),$db2//descendant-or-self::element())
return
   integrate($alla,$da1,$da2)
