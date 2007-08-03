declare function complete($al as xs:string*, $bl as xs:string*)
as element(list)*
{
for $a at $pa in $al
,$b at $pb in $bl
let $arest := subsequence($al,$pa+1)
,$brest := subsequence($bl,$pb+1)
return
<list>
<a pos="{$pa}">{$a}</a>
<arest>{$arest}</arest>
<b pos="{$pb}">{$b}</b>
<brest>{$brest}</brest>
</list>
};

<result>{
let $a := ("x","y")
,$b := ("p","q")
return
complete($a,$b)
}</result>
