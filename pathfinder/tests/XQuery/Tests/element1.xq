let $a := element foo { text {"foo"}, element bar { text {"bar"}}}
return element {$a}{$a//bar}
