for $a in (1, 1, 0), $b in (1, 1, 1) return if ($a) then if ($b) then "input fits (1)" else count(doc("does not exist")//*) else if ($b) then () else "input fits (0)"
