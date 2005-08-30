let $a := doc("voc.xml")//voyage[leftpage/boatname="ZEELANDIA"]
return 
<ship>
  <name> { distinct-values($a/leftpage/boatname) }</name>
  <visited> { 
    distinct-values($a//harbour/text())
  } </visited>
  <crew> {
    for $t in $a
    let $c := sum(
        for $m in $t/rightpage/onboard//*/text()
        let $s := exactly-one($m) cast as xs:integer
        return $s)
    where $c > 0
    return 
      <voyage> { 
        $t/leftpage/departure,
        <onboard> { $c } </onboard> 
      } </voyage>
  } </crew>
</ship>
