import rpc-module namespace soap = "http://www.cwi.nl/~zhang/soap/" at "http://www.cwi.nl/~zhang/soap/soap.xq";

for $i in (40, 50)
    return soap:add("localhost", $i, (10, 30))
