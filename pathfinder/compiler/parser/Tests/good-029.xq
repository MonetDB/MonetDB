(: collation must be parsed as a QName here :)
for $a in (1,2,3)
  order by collation
  return 42
