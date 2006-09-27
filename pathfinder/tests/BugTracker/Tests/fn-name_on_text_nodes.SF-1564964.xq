let $doc := <a>foo</a>
return
for $elem in $doc/descendant-or-self::node()
return <res>{$elem/name()}</res>
