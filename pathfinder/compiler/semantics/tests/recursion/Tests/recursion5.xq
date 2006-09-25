(: Recursion is a Pathfinder extension :)

(: Recursion is only legal on node types. :)
with $v as xs:integer seeded by 42
  recurse if ($v < 3) then 3 else ()
