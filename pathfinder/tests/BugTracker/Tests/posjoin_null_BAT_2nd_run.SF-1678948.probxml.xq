module namespace pxml = "http://www.cs.utwente.nl/~keulen";

(: Conversion to PXML :)

declare function pxml:wrapincertainpxml($node as node()*)
   as element(prob)
{
   <prob><poss prob="1.0">{$node}</poss></prob>
};
   
declare function pxml:xmltopxml($node as node())
   as element(prob)
{
   typeswitch($node)
      case $n as element() return
         let $name:=exactly-one($n/name())
         let $newchildren := for $child in $n/child::node() return pxml:xmltopxml($child)
         return pxml:wrapincertainpxml(element {$name} {$newchildren})
      case $n as attribute() return 
         pxml:wrapincertainpxml($n)
      case $n as text() return 
         pxml:wrapincertainpxml($n)
      default return pxml:wrapincertainpxml(<error/>)
};

(: Presentation with dot :)

declare function pxml:makelines($lines as xs:string*)
   as element(line)*
{
   for $line in $lines return <line>{$line}</line>
};

declare function pxml:ToDot($node as element())
   as element(line)*
{
   let $all := $node/descendant-or-self::element()
      ,$start := ("strict graph prob {"
                 ,"nodesep=0.1"
                 ,"ranksep=0.2"
                 )
      ,$end := "}"
   return
      pxml:makelines(
      ($start
      ,for $n at $p in $all
       let $name := concat("n",string($p))
          ,$type := exactly-one($n/name())
       return 
         string-join(($name," ",
            if ($type eq "prob")
            then '[label="" shape=triangle orientation=180 width=0.2 height=0.2]'
            else if ($type eq "poss")
            then '[label="" shape=circle fontsize=8 width=0.2 height=0.2]'
            else if ($type eq "")
            then ('[label="',string($n),'" shape=ellipse style=filled fontsize=8 width=0.2 height=0.2]')
            else ('[label="[',$type,']" shape=ellipse style=filled fontsize=8 width=0.2 height=0.2]')
         ),"")
      ,for $n at $pn in $all
          ,$c in $n/child::element()
       return
          let $pc := exactly-one(pxml:index-of($all,$c))
             ,$nname := concat("n",string($pn))
             ,$cname := concat("n",string($pc))
          return
            string-join((
               $nname," -- ",$cname,
               if (exactly-one($c/name()) eq "poss")
               then (' [label="',string($c/@prob),'" fontsize=8]')
               else ' [fontsize=8]'
            ),"")
      ,$end
      ))
};

(: Configuration :)

declare function pxml:getNonMultipleNames($config as element(config)) as xs:string*
{
   for $n in $config//nonmultiple/text()
   return string($n)
};

(: Integration :)

declare function pxml:product($seq as xs:decimal*)
   as xs:decimal
{
   if (count($seq) gt 0)
   then exactly-one($seq[1]) * pxml:product($seq[position() gt 1])
   else 1.0
};

declare function pxml:product_int($seq as xs:integer*)
   as xs:integer
{
   if (count($seq) gt 0)
   then exactly-one($seq[1]) * pxml:product_int($seq[position() gt 1])
   else 1
};

declare function pxml:deep-equal($node1 as node(), $node2 as node())
   as xs:boolean
{
   let $e1 := for $e in $node1/descendant-or-self::element()
              return ($e/name(),$e/text())
      ,$e2 := for $e in $node2/descendant-or-self::element()
              return ($e/name(),$e/text())
   return
      (count($e1) eq count($e2))
      and
      (string-join($e1,",") eq string-join($e2,","))
};

declare function pxml:index-of($all as element()*, $elems as element()*)
   as xs:integer*
{
   for $a at $p in $all
   where some $e in $elems satisfies $a is $e
   return $p
};

declare function pxml:delete($elems as xs:integer*, $elem as xs:integer*)
   as xs:integer*
{
   for $e in $elems
   where not($e=$elem)
   return $e
};

declare function pxml:seqIntCand($xlist as xs:integer*
                                ,$ylist as xs:integer*
                                ,$impossible as xs:string*
                                ,$mustbe as element(cand)*)
   as element(list)*
{
   let $fxlist := pxml:delete($xlist,$mustbe/@a)
      ,$fylist := pxml:delete($ylist,$mustbe/@b)
   return
      for $l in pxml:seqIntCand1($fxlist,$fylist,$impossible)
      return <list>{$mustbe,$l/cand}</list>
};

declare function pxml:seqIntCand1($xlist as xs:integer*
                                 ,$ylist as xs:integer*
                                 ,$impossible as xs:string*)
   as element(list)*
{
   if (count($xlist) eq 0)
   then <list>{for $y in $ylist return <cand a="{$y}"/>}</list>
   else if (count($ylist) eq 0)
   then <list>{for $x in $xlist return <cand a="{$x}"/>}</list>
   else
      let $x := exactly-one($xlist[1])
         ,$xs := $xlist[position() gt 1]
      return
        (pxml:seqIntCand2($x,$xs,$ylist,$impossible)
        ,for $l in pxml:seqIntCand1($xs,pxml:delete($ylist,$x),$impossible)
         return <list>{<cand a="{$x}"/>,$l/cand}</list>
        )
   };

declare function pxml:seqIntCand2($x as xs:integer
                                 ,$xs as xs:integer*
                                 ,$ylist as xs:integer*
                                 ,$impossible as xs:string*)
   as element(list)*
{
   for $y in $ylist
   return
      let $ysplusydone := pxml:delete($ylist,$y)
         ,$pairrest := pxml:seqIntCand1(pxml:delete($xs,$y),pxml:delete($ysplusydone,$x),$impossible)
      return
         if (($x eq $y) or (concat($x,",",$y) = $impossible))
         then ()
         else for $l in $pairrest
              return <list>{<cand a="{$x}" b="{$y}"/>,$l/cand}</list>
};

declare function pxml:constructImpossibleMatches($config as element(config)
                                                ,$l1 as node()*
                                                ,$l2 as node()*)
   as xs:string*
{
   let $noteq := ("",pxml:getNonMultipleNames($config))
      ,$offset := count($l1)
   for $e1 at $p1 in $l1
      ,$e2 at $p2 in $l2
   return
   let $pair:=concat($p1,",",$p2+$offset)
      ,$e1n:=$e1/name()
      ,$e2n:=$e2/name()
   return
      if ($e1n ne $e2n)
      then $pair
      else if ($e1n eq "movie")
           then let $t1 := exactly-one($e1/title/text())
                   ,$t2 := exactly-one($e2/title/text())
                return
                  if (some $ne in $noteq
                      satisfies (contains($t1,$ne) and contains($t2,$ne)))
                  then ()
                  else $pair
           else if ($e1n eq "genre"
                    and $e1/text() != $e2/text())
                then $pair
                else ()
};

declare function pxml:constructMustBeMatches($config as element(config)
                                            ,$l1 as node()*
                                            ,$l2 as node()*)
   as element(cand)*
{
   let $nonmultiple_names:=("",pxml:getNonMultipleNames($config))
      ,$offset := count($l1)
   return 
      for $e1 at $p1 in $l1
         ,$e2 at $p2 in $l2
      where ($e1/name() eq $e2/name())
      return
         if (($e1/name() = $nonmultiple_names) or pxml:deep-equal($e1,$e2))
         then <cand a="{$p1}" b="{$offset+$p2}"/>
         else ()
};


declare function pxml:countNames($all as node()*
                                ,$names as xs:string*
                                ,$x as element(list))
   as xs:integer*
{
   for $n in $names
   let $positions := (for $e at $p in $all
                      return if ($e/name() eq $n) then $p else ()
                     )
   return
      count($x/cand[./@a=$positions])
};

declare function pxml:theOracle_list($config as element(config)
                                    ,$all as node()*
                                    ,$x as element(list))
   as xs:decimal
{
   let $nonmultiple_names:=("",pxml:getNonMultipleNames($config))
   return
      if (pxml:countNames($all,$nonmultiple_names,$x)>1)
      then 0.0
      else
         pxml:product(
         for $cand in $x/cand
         return
            if ($cand/@b)
            then
                 (let $a := exactly-one($all[string($cand/@a) cast as xs:integer])
                     ,$b := exactly-one($all[string($cand/@b) cast as xs:integer])
                  return
                     if ($a/name() eq "" and $b/name() eq "")
                     then if (string($a) eq string($b)) then 1.0 else 0.5
                     else if ($a/name() eq $b/name()) then 0.7 else 0.0
                 )
            else 1.0
         )
};

declare function pxml:filterCandidates($config as element(config)
                                      ,$all as node()*
                                      ,$xlist as element(list)*)
   as element(list)*
{
   for $l in $xlist
   let $prob:=pxml:theOracle_list($config,$all,$l)
   where $prob gt 0
   return <list prob="{$prob}">{$l/cand}</list>
};

declare function pxml:constructPossibilities($config as element(config)
                                            ,$all as node()*
                                            ,$e as xs:string
                                            ,$sumprob as xs:double
                                            ,$xlist as element(list)*)
   as element()*
{
   for $l in $xlist
   let $pl := $l/@prob
   return (
(:
      <debug sum="{$sumprob}">{$l}</debug>,
:)
        <poss prob="{$pl div $sumprob}">{
          if ($e eq "")
          then text {pxml:constructPossibility($config,$all,$l)}
          else element {$e} {pxml:constructPossibility($config,$all,$l)}
       }</poss>)
};

declare function pxml:constructPossibility($config as element(config)
                                          ,$all as node()*
                                          ,$l as element(list))
   as element()*
{
   for $cand in $l/cand
   return
      if (count($cand/@b) gt 0)
      then
         if (($cand/@a > count($all)) or ($cand/@b > count($all)))
         then <error>{$cand} not found in <all>{$all}</all></error>
         else
           (let $e1:=exactly-one($all[string($cand/@a) cast as xs:integer])
               ,$e2:=exactly-one($all[string($cand/@b) cast as xs:integer])
               ,$e1e:=$e1/self::element()
               ,$e2e:=$e2/self::element()
           return
              if (count(($e1e,$e2e)) gt 0)
              then pxml:integrate_e($config,$e1e,$e2e)
              else pxml:integrate_t($config,$e1,$e2))
      else
         pxml:xmltopxml(exactly-one($all[string($cand/@a) cast as xs:integer]))
};

declare function pxml:integrate_e($config as element(config)
                                 ,$pta as element()
                                 ,$ptb as element())
   as element(prob)
{
   let $ea:=$pta/name()
      ,$eb:=$ptb/name()
      ,$ptsa := $pta/child::node()
      ,$ptsb := $ptb/child::node()
      ,$impossible := pxml:constructImpossibleMatches($config,$ptsa,$ptsb)
      ,$mustbe := pxml:constructMustBeMatches($config,$ptsa,$ptsb)
      ,$all := ($ptsa,$ptsb)
      ,$ptsacode:=(1 to count($ptsa))
      ,$ptsbcode:=(count($ptsa)+1 to count($all))
      ,$allerr := ($all,<error/>,<error/>)
      ,$sic:=pxml:seqIntCand($ptsacode,$ptsbcode,$impossible,$mustbe)
      ,$filteredcandidates:=pxml:filterCandidates($config,$allerr,$sic)
      ,$sumprob := sum(for $l in $filteredcandidates return $l/@prob)
   return
      <prob>{(
<integrate>{$config}<one>{$pta}</one><two>{$ptb}</two>
           <mustbe>{$mustbe}</mustbe><impossible>{$impossible}</impossible>
           <sic>{$sic}</sic>
           {$filteredcandidates}</integrate>,
      if ($ea eq $eb)
      then pxml:constructPossibilities($config,$all,$ea,$sumprob,$filteredcandidates)
      else (<poss prob="0.5">{pxml:xmltopxml($pta)}</poss>,
            <poss prob="0.5">{pxml:xmltopxml($ptb)}</poss>)
   )}</prob>
};

declare function pxml:integrate_t($config as element(config)
                                 ,$pta as node()
                                 ,$ptb as node())
   as element(prob)
{
   if (string($pta) eq string($ptb))
   then <prob><poss prob="1.0">{$pta}</poss></prob>
   else <prob><poss prob="0.5">{$pta}</poss><poss prob="0.5">{$ptb}</poss></prob>
};

declare function pxml:countPWs($pt as element(prob))
   as xs:integer
{
   sum (
      for $poss in $pt/child::poss
      return
         pxml:product_int (
            for $xml in $poss/child::node()
            return
               pxml:product_int(
                  for $prob in $xml/child::prob
                  return pxml:countPWs($prob) ) ) )
};


declare function pxml:constructPWs($pt as element(prob))
   as element(world)*
{
   for $poss in $pt/child::poss
   let $prob := $poss/@prob
   for $pw in pxml:constructPWs_poss($poss)
   return
      <world prob="{$prob*($pw/@prob)}">
         {$pw/child::node()}
      </world>
};

declare function pxml:constructPWs_poss($pt as element(poss))
   as element(world)*
{
   let $worldlists := (for $xml in $pt/child::node()
                       return <worldlist>{pxml:constructPWs_xml($xml)}</worldlist>)
      ,$combs := pxml:allCombinations($worldlists)
   return
      for $comb in $combs
      let $prob := pxml:product($comb/world/@prob)
      return
         <world prob="{$prob}">{$comb/world/child::node()}</world>
};

declare function pxml:constructPWs_xml($pt as node())
   as element(world)*
{
   if ($pt instance of text())
   then <world prob="1">{$pt}</world>
   else
      let $worldlists := (for $prob in $pt/child::prob
                          return <worldlist>{pxml:constructPWs($prob)}</worldlist>)
         ,$combs := pxml:allCombinations($worldlists)
      return
         if (count($combs) eq 0)
         then <world prob="1">{$pt}</world>
         else
            for $comb in $combs
            let $b := pxml:product($comb/world/@prob)
            return
               <world prob="{$b}">{
                  element {$pt/name()} {$comb/world/child::node()}
               }</world>
};

declare function pxml:allCombinations($worldlists as element(worldlist)*)
   as element(worldlist)*
{
   let $cnt := count($worldlists)
   return 
   if ($cnt eq 0)
   then ()
   else if ($cnt eq 1)
   then for $x in $worldlists/child::world return <worldlist>{$x}</worldlist>
   else
      let $y := exactly-one($worldlists[1])
         ,$ys := $worldlists[position() gt 1]
         ,$as := pxml:allCombinations($ys)
      return 
         for $x in $y/child::world, $a in $as
         return <worldlist>{$x}{$a/child::world}</worldlist>
};

declare function pxml:rank_results($worlds as element(world)*)
   as element(answer)*
{
   for $v in distinct-values($worlds/descendant::text())
   let $ws := $worlds[./descendant::text()[string(.)=$v]]
      ,$rank := sum($ws/@prob)
   order by $rank descending
   return
      <answer rank="{$rank}">{$v}</answer>
};

