let $a := (1, 2.3, "test", element bar {text{"bl"},text{"ub"}}) return for $b in ("foo", "bar", "baz") return attribute {$b}{$a,$b}
