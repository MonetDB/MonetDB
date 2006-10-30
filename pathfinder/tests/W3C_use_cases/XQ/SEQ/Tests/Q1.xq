for $s in doc("report1.xml")//section[section.title = "Procedure"]
return ($s//incision)[2]/instrument
