import rpc-module namespace foo = "http://www.cwi.nl/~zhang/xquery-rpc/" at "http://www.cwi.nl/~zhang/xquery-rpc/xquery-rpc.xq";

for $i in ("hello", "nihao")
    return foo:concatStr("localhost", $i, ", this message is sent back by the XQueryRPC receiver.")
