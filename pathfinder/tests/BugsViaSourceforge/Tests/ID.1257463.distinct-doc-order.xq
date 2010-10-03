let $a :=
<a>
<b c="foo"/>
<d/>
</a>
return $a/(d|b/@c)
