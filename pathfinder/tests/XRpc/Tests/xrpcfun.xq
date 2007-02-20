module namespace xrpcfun = "xrpc-fun";

declare function xrpcfun:convert($v as xs:decimal?) as xs:decimal?
{
    (: convert Dfl to Euro :)
    2.20371 * $v
};

declare function xrpcfun:xrpcConvert($u as xs:string, $v as xs:decimal?) as xs:decimal?
{
    execute at {$u}{xrpcfun:convert($v)}
};

declare function xrpcfun:add($v1 as xs:decimal, 
                          $v2 as xs:decimal*) as xs:decimal* 
{
    let $res := $v1 
    for $i in $v2  
        return $res + $i 
}; 

declare function xrpcfun:concatStr($v1 as xs:string, 
                                $v2 as xs:string) as xs:string
{
    concat($v1, $v2)
};

declare function xrpcfun:returnAncestor($v as xs:anyNode*) 
    as xs:anyNode*
{ 
    $v/ancestor::node()
}; 

declare function xrpcfun:returnAncestorOrSelf($v as xs:anyNode*) 
    as xs:anyNode*
{ 
    $v/ancestor-or-self::node()
}; 

declare function xrpcfun:returnChild($v as xs:anyNode*)
    as xs:anyNode*
{
    $v/child::*
};

declare function xrpcfun:returnDescendant($v as xs:anyNode*)
    as xs:anyNode*
{
    $v/descendant::node()
};

declare function xrpcfun:returnDescendantOrSelf($v as xs:anyNode*)
    as xs:anyNode*
{
    $v/descendant-or-self::node()
};

declare function xrpcfun:returnFollowing($v as xs:anyNode*)
    as xs:anyNode*
{
    $v/following::node()
};

declare function xrpcfun:returnFollowingSibling($v as xs:anyNode*)
    as xs:anyNode*
{
    $v/following-sibling::node()
};

declare function xrpcfun:returnPreceding($v as xs:anyNode*)
    as xs:anyNode*
{
    $v/preceding::node()
};

declare function xrpcfun:returnPrecedingSibling($v as xs:anyNode*)
    as xs:anyNode*
{
    $v/preceding-sibling::node()
};

declare function xrpcfun:returnParent($v as xs:anyNode*) 
    as xs:anyNode*
{ 
    $v/parent::node()
}; 

declare function xrpcfun:returnSelf($v as xs:anyNode*) 
    as xs:anyNode*
{ 
    $v/self::node()
};

declare function xrpcfun:echoVoid()
{
    ()
};

declare function xrpcfun:echoVoidWithParams($v1 as xs:string*, 
                                         $v2 as xs:anyNode*, 
                                         $v3 as xs:integer*)
{
    ()
};

declare function xrpcfun:echoAll($n as xs:anyType*) as xs:anyType*
{ $n };

declare updating function xrpcfun:insertNode($n as xs:anyNode*)
{ do insert $n into exactly-one(doc("hello.xml")//world) };
