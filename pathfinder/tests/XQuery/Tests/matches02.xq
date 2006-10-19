(: 'flags' containing undefined characters should trigger err:FORX001 :)

for $i in ("invalidflags1", "ismx", "invalidflags2")
return fn:matches("abc", "abc", $i)
