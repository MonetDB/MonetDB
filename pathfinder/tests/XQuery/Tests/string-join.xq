(let $a := ("foo", "bar", "baz", "blub") return for $b in (", ", "-", "") return string-join($a, $b),
 for $b in (", ", "-", "") return string-join((), $b))
