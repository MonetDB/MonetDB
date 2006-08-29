module namespace func = "http://www.monetdb.nl/basicfunctions";

declare function func:convert($v as xs:decimal?) as xs:decimal?
{
    (: convert Dfl to Euro :)
    2.20371 * $v
};

declare function func:add($v1 as xs:decimal, 
                          $v2 as xs:decimal*) as xs:decimal* 
{
    let $res := $v1 
    for $i in $v2  
        return $res + $i 
}; 

declare function func:concatStr($v1 as xs:string, 
                                $v2 as xs:string) as xs:string
{
    concat($v1, $v2)
};

declare function func:returnAncestor($v as xs:anyNode*) 
    as xs:anyNode*
{ 
    $v/ancestor::node()
}; 

declare function func:returnAncestorOrSelf($v as xs:anyNode*) 
    as xs:anyNode*
{ 
    $v/ancestor-or-self::node()
}; 

declare function func:returnChild($v as xs:anyNode*)
    as xs:anyNode*
{
    $v/child::node()
};

declare function func:returnDescendant($v as xs:anyNode*)
    as xs:anyNode*
{
    $v/descendant::node()
};

declare function func:returnDescendantOrSelf($v as xs:anyNode*)
    as xs:anyNode*
{
    $v/descendant-or-self::node()
};

declare function func:returnFollowing($v as xs:anyNode*)
    as xs:anyNode*
{
    $v/following::node()
};

declare function func:returnFollowingSibling($v as xs:anyNode*)
    as xs:anyNode*
{
    $v/following-sibling::node()
};

declare function func:returnPreceding($v as xs:anyNode*)
    as xs:anyNode*
{
    $v/preceding::node()
};

declare function func:returnPrecedingSibling($v as xs:anyNode*)
    as xs:anyNode*
{
    $v/preceding-sibling::node()
};

declare function func:returnParent($v as xs:anyNode*) 
    as xs:anyNode*
{ 
    $v/parent::node()
}; 

declare function func:returnSelf($v as xs:anyNode*) 
    as xs:anyNode*
{ 
    $v/self::node()
};

declare function func:echoVoid()
{
    ()
};

declare function func:echoVoidWithParams($v1 as xs:string*, 
                                         $v2 as xs:anyNode*, 
                                         $v3 as xs:integer*)
{
    ()
};
