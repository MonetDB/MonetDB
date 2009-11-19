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
          ( some $c in $ep//assistant_supervisors_family satisfies contains(upper-case($c),  "RENSINK"))
          or
          ( some $c in $ep//supervisors_family satisfies contains(upper-case($c),  "RENSINK"))
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
 

    ,$grpvals0  := for  $ep  in distinct-values($eplvl0//type/text())
                               order by $ep descending
                               return $ep

    ,$grpvals1  := for  $ep  in distinct-values($eplvl0//year/text())
                               order by $ep descending
                               return $ep
                      


   ,$head := (
   )
 
 
 

          ,$toc := (: ecv:grouped-toc($eplvl0) :)
          	 	     

    let  $grpitems0 := for $g0 in $grpvals0
                          let $eplvl1 := for $ep in $eplvl0
                                                  where $ep//type = $g0
                                                  return $ep
                          where not(empty($eplvl1))
                          return
                          (

    let  $grpitems1 := for $g1 in $grpvals1
                          let $eplvl2 := for $ep in $eplvl1
                                                  where $ep//year = $g1
                                                  return $ep
                          where not(empty($eplvl2))
                          return
                          (

                          )
    return if (not(empty($grpitems1)))
           then  $grpitems1
           else  ()
                          )
    return if (not(empty($grpitems0)))
           then  $grpitems0
           else  ()
 



	,$items :=  (: ecv:grouped-view($eplvl0) :)

    for $g0 in $grpvals0
    let $eplvl1 := for $ep in $eplvl0
                            where $ep//type = $g0
                            return $ep
    where not(empty($eplvl1))
    return
           <div class="type" id="{$g0}"> { (
             <h1>{ecv:to-human($g0, $human_type)}</h1>,

    for $g1 in $grpvals1
    let $eplvl2 := for $ep in $eplvl1
                            where $ep//year = $g1
                            return $ep
    where not(empty($eplvl2))
    return
           <div class="year" id="{$g0}::{$g1}"> { (
             ecv:print-eprint($eplvl2)
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

