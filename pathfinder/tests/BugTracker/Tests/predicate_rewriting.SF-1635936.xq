declare namespace xx = 'http://www.w3.org/2005/xpath-functions';

let $d :=
<a>
<b>
<c x='3'>no hit</c>
</b>
<b>
<c x='2'>no hit</c>
</b>
<b>
<c x='1'>hit!</c>
</b>
</a>
return
(
$d/b/c[@x = xx:position()]
,
($d/b/c)[@x = xx:position()]
)

