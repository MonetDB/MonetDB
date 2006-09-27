declare function namestring($node as element()) as xs:string
{
let $n1 := $node/descendant-or-self::element()/name()
return string-join($n1,",")
};

let $doc := <movies><movie><title>King Kong</title><year>1933</year></movie></movies>
return namestring($doc)
