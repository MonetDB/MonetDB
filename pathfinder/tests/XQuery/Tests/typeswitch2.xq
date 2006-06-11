(
for $a at $b in ("foo", 21, 1, 21, <a>4</a>)
return typeswitch (if ($b mod 2 = 0) 
                   then (if ($b mod 4 = 0) 
                         then (1,2,3) 
                         else ())
                   else $a)
                  case empty () return 23
                  default return $a

, 100, 

for $a at $b in ("foo", 21, 1, 21, <a>4</a>)
return typeswitch (if ($b mod 2 = 0) 
                   then (if ($b mod 4 = 0) 
                         then (1,2,3) 
                         else ())
                   else $a)
                  case xs:integer? return 23
                  default return $a

, 100, 

for $a at $b in ("foo", 21, 1, 21, <a>4</a>)
return typeswitch (if ($b mod 2 = 0) 
                   then (if ($b mod 4 = 0) 
                         then (1,2,3) 
                         else ())
                   else $a)
                  case xs:integer return 23
                  default return $a

, 100, 

for $a at $b in ("foo", 21, 1, 21, <a>4</a>)
return typeswitch (if ($b mod 2 = 0) 
                   then (if ($b mod 4 = 0) 
                         then (1,2,3) 
                         else ())
                   else $a)
                  case xs:integer* return 23
                  default return $a

, 100, 

for $a at $b in ("foo", 21, 1, 21, <a>4</a>)
return typeswitch (if ($b mod 2 = 0) 
                   then (if ($b mod 4 = 0) 
                         then (1,2,3) 
                         else ())
                   else $a)
                  case xs:integer+ return 23
                  default return $a
)
