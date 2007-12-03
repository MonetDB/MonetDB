let $c := <a><t/><t/><p/><t><p/></t></a>
for $doc in $c//p
  return $doc/preceding-sibling::*
