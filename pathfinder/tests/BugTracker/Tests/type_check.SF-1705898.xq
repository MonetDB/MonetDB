let $min :=  123.4

let $max := 5678.9

let $bins := 400

let $grp := ($max - $min) idiv $bins

return element { "result" } { $grp }
