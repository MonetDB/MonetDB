for $a in (1, 1, 1), $b in (1, 1, 0) return if ($a) then if ($b) then "input fits (1)" else count(doc("foo.xml")//*) else if ($b) then () else "input fits (0)"
