declare function getCVSSfield($vec as xs:string?, $field as xs:string)
   as xs:string*
{
   let $f := concat($field,":")
   let $slen := string-length($vec)
   let $flen := string-length($f)
   for $i in (1 to $slen)
   return
      if (substring($vec,$i,$flen) eq $f)
      then substring($vec,$i+$flen,1)
      else ()
};

<result>{
(:
let $col := collection("nvdcve")
:)
let $col := for $y in (2002 to 2007) return doc(concat(concat("nvdcve-",$y),".xml"))
let $allentries := $col//*:entry
let $rejected := $allentries[./*:desc/*:descript[contains(.,"** REJECT **")]]
let $entries := $allentries except $rejected
let $fieldnames := ("A","AC","AV","Au","C","I")
for $field in $fieldnames
let $vals := 
   for $e in $entries
   let $vec := string($e/@CVSS_vector)
   let $val := getCVSSfield($vec,$field)
   return $val
let $aantvals := count($vals)
return
   <field field="{$field}">{
      for $v in distinct-values($vals)
      let $cnt := count($vals[. eq $v])
      let $perc := round(1000*($cnt div $aantvals)) div 10
      return
         <access ac_val="{$v}" ac_count="{$cnt}" perc="{$perc}"/>
   }</field>
}</result>
