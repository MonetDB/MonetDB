module namespace ecv = "http://www.cs.utwente.nl/~belinfan/ecv";
(: this file contains code to apply a given view  :)


declare function ecv:print-eprint($entries as element()*) as element()*
{
	$entries//fmt/div
};

(: ======================================================================= :)

declare function ecv:generate($doc as xs:string)
   as element(tuple)*
{
   for $ep in doc($doc)//tuple
   where (
     (
       (
          ( some $c in $ep//research_groups satisfies contains(upper-case($c),  "EWI-FMT"))
       )
     )
   )
   return $ep
};

declare function ecv:apply-view()
   as element()
{
	ecv:view-from-save("eprints-export-conv.xml")
};

declare function ecv:view-from-save($doc as xs:string)
   as element()
{
           let $eplvl0 := ecv:generate($doc)

    ,$human_type := <m>      <e k="mastersthesis">Master's Thesis</e>
           <e k="article">Article</e>
           <e k="inaugural_lecture">Inaugural lecture</e>
           <e k="thesis">PhD Thesis</e>
           <e k="conference_item">Conference or Workshop Paper</e>
           <e k="book_review">Review</e>
           <e k="book_section">Book Section</e>
           <e k="intreport">Internal Report</e>
           <e k="manual">Manual</e>
           <e k="book">Book</e>
           <e k="patent">Patent</e>
           <e k="extreport">External Report</e>
     </m>
 

    ,$grpvals0  := for  $ep  in distinct-values($eplvl0//year/text())
                               order by $ep ascending
                               return $ep

    ,$grpvals1  := for  $ep  in distinct-values($eplvl0//type/text())
                               order by $ep ascending
                               return $ep

    ,$grpvals2  := for  $ep  in distinct-values($eplvl0//creators_family/text())
                               order by $ep ascending
                               return $ep
                      


   ,$head := (
<li><h2>year</h2>
 <ul>
<li><h2>type</h2>
 <ul>
<li><h2>creators_family</h2>
 </li>
 </ul>
 </li>
 </ul>
 </li>
   )
 
 
 

          ,$toc := (: ecv:grouped-toc($eplvl0) :)
          	 	     

    let  $grpitems0 := for $g0 in $grpvals0
                          let $eplvl1 := for $ep in $eplvl0
                                                  where $ep//year = $g0
                                                  return $ep
                          where not(empty($eplvl1))
                          return
                          (
                          <li><a href="#{$g0}">{$g0}</a> 
                          {

    let  $grpitems1 := for $g1 in $grpvals1
                          let $eplvl2 := for $ep in $eplvl1
                                                  where $ep//type = $g1
                                                  return $ep
                          where not(empty($eplvl2))
                          return
                          (
                          <li><a href="#{$g0}::{$g1}">{ecv:to-human($g1, $human_type)}</a> 
                          {

    let  $grpitems2 := for $g2 in $grpvals2
                          let $eplvl3 := for $ep in $eplvl2
                                                  where $ep//creators_family = $g2
                                                  return $ep
                          where not(empty($eplvl3))
                          return
                          (
                          <li><a href="#{$g0}::{$g1}::{$g2}">{$g2}</a> 

                          </li>
                          )
    return if (not(empty($grpitems2)))
           then  <ul>{$grpitems2}</ul>
           else  ()
                          }
                          </li>
                          )
    return if (not(empty($grpitems1)))
           then  <ul>{$grpitems1}</ul>
           else  ()
                          }
                          </li>
                          )
    return if (not(empty($grpitems0)))
           then  <ul>{$grpitems0}</ul>
           else  ()
 



	,$items :=  (: ecv:grouped-view($eplvl0) :)

    for $g0 in $grpvals0
    let $eplvl1 := for $ep in $eplvl0
                            where $ep//year = $g0
                            return $ep
    where not(empty($eplvl1))
    return
           <div class="year" id="{$g0}"> { (
             <h1>{$g0}</h1>,

    for $g1 in $grpvals1
    let $eplvl2 := for $ep in $eplvl1
                            where $ep//type = $g1
                            return $ep
    where not(empty($eplvl2))
    return
           <div class="type" id="{$g0}::{$g1}"> { (
             <h2>{ecv:to-human($g1, $human_type)}</h2>,

    for $g2 in $grpvals2
    let $eplvl3 := for $ep in $eplvl2
                            where $ep//creators_family = $g2
                            return $ep
    where not(empty($eplvl3))
    return
           <div class="creators_family" id="{$g0}::{$g1}::{$g2}"> { (
             <h3>{$g2}</h3>,
             ecv:print-eprint($eplvl3)
           ) } </div>
           ) } </div>
           ) } </div>
 
			              
           return
           	<div>{
  
          	 		( if (empty($toc))
          	 		   then ()
          	 		   else <div class="toc">{$toc}</div>
          	 		,
          	 		   if (empty($items))
          	 		   then ()
          	 		   else <div class="items">{$items}</div>
          	 		)
           	}</div>
};


(:
 declare function try($v as xs:string) as xs:string { let $a := <t><n t="a">Aap jaap</n><n t="b">Beep piep piep</n></t> , $r := for  $x in $a//n where $x/@t eq $v return $x/text() return if (empty($r)) then $v  else string-join($r/string(), "") } ; <div><p>{try("a")}</p><p>{try("b")}</p><p>{try("c")}</p></div>
  :)

 declare function ecv:to-human($val as xs:string, $m as element())
   as xs:string
{
   let $r := for $x in $m//e where $x/@k eq $val
           return $x
  return
      if (empty($r)) 
      then $val
      else string-join($r/string(), "")
};

