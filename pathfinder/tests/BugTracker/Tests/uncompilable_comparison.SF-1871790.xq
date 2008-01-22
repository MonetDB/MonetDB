for $x in doc("x.xml")//a
return
if (exactly-one($x/a/@c) != exactly-one($x/b/@c)) then
$x
else
()
