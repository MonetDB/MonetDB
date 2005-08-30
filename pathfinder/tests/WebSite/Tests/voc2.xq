  for $t in doc("voc.xml")//voyage[leftpage/boatname="ZEELANDIA"]
  let $l := $t/leftpage
  order by $l/departure
  return 
    <voyage> {
      $l/departure/text(), text { ", " }, $l/harbour/text()
    } </voyage>
