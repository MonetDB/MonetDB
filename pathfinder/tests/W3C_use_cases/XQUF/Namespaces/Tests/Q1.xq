declare namespace nara = "http://www.anr.fr/nara";

for $e in doc("grant.xml")//*
where not (namespace-uri($e) eq "http://www.anr.fr/nara")
return 
  do rename $e 
      as QName("http://www.anr.fr/nara", 
               concat("nara:",local-name($e)))
