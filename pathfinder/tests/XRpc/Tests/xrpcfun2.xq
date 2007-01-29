module namespace bar = "bar";

import module namespace foo = "xrpc-fun"
    at
    "/ufs/zhang/src/monet/dev/pathfinder/tests/XRpc/Tests/xrpcfun.xq";

declare function bar:xrpcConvert($v as xs:decimal?) as xs:decimal?
{
    execute at {"localhost"}{foo:convert($v)}

};

