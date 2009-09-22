let $query := tijah:tokenize(fn:string("aap beer noot"))
return fn:string-length($query)
<>
for $n in (1,2)
let $query := tijah:tokenize(fn:string("aap beer noot"))
let $len   := fn:string-length($query)
return ($len)
