import rpc-module namespace foo = "http://www.cwi.nl/~zhang/xquery-rpc/" at "http://www.cwi.nl/~zhang/xquery-rpc/xquery-rpc.xq";

for $i in (40.5, 50.5)
    return foo:add("localhost", $i, (10.5, 30.5))
