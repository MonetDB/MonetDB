let $a := (1,2) return 
for $b in ($a, 3) return 
  if (doc("foo.xml")/a) 
  then for $c in (1,0,1,0) 
       return if ($c) 
	      then ($a, $c) 
              else ("foo", $c)
  else "not in result"
