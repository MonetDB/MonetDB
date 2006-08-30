declare function convmonth($mon as xs:string)
{
   let $monthnum := <tab><m n="jan">01</m><m n="feb">02</m><m n="mar">03</m>
      <m n="apr">04</m><m n="may">05</m><m n="jun">06</m><m n="jul">07</m>
      <m n="aug">08</m><m n="sep">09</m><m n="oct">10</m><m n="nov">11</m>
      <m n="dec">12</m></tab>
   return
      $monthnum/m[@n=$mon]
};

(
<test>
{convmonth("apr")}
</test>
,
<test>
{for $a in ("apr") return convmonth($a)}
</test>
,
<test>
{for $a in ("dec","jan") return convmonth($a)}
</test>
,
<test>
{for $a in ("apr","dec","jan") return convmonth($a)}
</test>
)
