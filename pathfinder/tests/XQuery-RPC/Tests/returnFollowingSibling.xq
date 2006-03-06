import rpc-module namespace foo = "http://www.cwi.nl/~zhang/xquery-rpc/" at "http://www.cwi.nl/~zhang/xquery-rpc/xquery-rpc.xq";

for $i in (1, 2)
    return foo:returnFollowingSibling("localhost", doc("http://www.cwi.nl/~zhang/xquery-rpc/hello.xml"))
