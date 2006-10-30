for $s in doc("report1.xml")//section[section.title = "Procedure"]
return ($s//instrument)[position()<=2]
