let $i2 := (doc("report1.xml")//incision)[2]
for $a in (doc("report1.xml")//action)[. >> exactly-one($i2)][position()<=2]
return $a//instrument
