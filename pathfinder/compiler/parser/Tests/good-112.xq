(: Scanner must not find the pattern `) as' in
   the order by clause.  See bug #1359816 :)
for $i in (1,2)
  order by ($i) ascending
  return $i
