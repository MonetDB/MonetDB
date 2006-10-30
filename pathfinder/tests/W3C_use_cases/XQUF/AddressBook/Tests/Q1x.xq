for $a in doc("archive.xml")/archived-agenda/entry, 
    $v1 in doc("copy1.xml")/agenda-version/entry, 
    $v2 in doc("copy2.xml")/agenda-version/entry
where $a/name = $v1/name
  and $v1/name = $v2/name
return 
  if ($a/contact = $v1/contact and $v1/contact=$v2/contact)
  then ()
  else 
    if ($v1/contact = $v2/contact)
    then ( do replace value of exactly-one($a/contact) with $v1/contact,
           do replace value of 
              exactly-one(doc("archive.xml")/*/last-synch-time) 
           with "2006-04-23T12:00" (:current-dateTime():)
         )
    else 
      if ($a/contact = $v1/contact)
      then (
            do replace value of exactly-one($a/contact) with $v2/contact,
            do replace value of exactly-one($v1/contact) with $v2/contact,
            do replace value of 
               exactly-one(doc("archive.xml")/*/last-synch-time)
            with "2006-04-23T12:00" (:current-dateTime():)
            )
      else 
        if ($a/contact = $v2/contact)
        then (
              do replace value of exactly-one($a/contact) with $v1/contact,
              do replace value of exactly-one($v2/contact) with $v1/contact,
              do replace value of 
                 exactly-one(doc("archive.xml")/*/last-synch-time)
              with "2006-04-23T12:00" (:current-dateTime():)
             )
        else (
          do insert 
            <fail>
               <arch>{ $a }</arch>
               <v1>{ $v1 }</v1>
               <v2>{ $v2 }</v2>
            </fail>
          into exactly-one(doc("log.xml")/log)
        )
