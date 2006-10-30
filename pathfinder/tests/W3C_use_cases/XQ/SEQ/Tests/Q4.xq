for $p in doc("report1.xml")//section[section.title = "Procedure"]
where not(some $a in $p//anesthesia satisfies
        $a << ($p//incision)[1] )
return $p
