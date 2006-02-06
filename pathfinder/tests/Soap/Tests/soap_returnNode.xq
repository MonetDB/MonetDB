import module namespace soap = "http://www.cwi.nl/~zhang/soap/" at "http://www.cwi.nl/~zhang/soap/soap.xq";

for $i in (1, 2)
    return soap:returnNode("localhost", doc("http://www.cwi.nl/~zhang/soap/returnNode.xml"))
