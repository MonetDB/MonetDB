import module namespace soap = "http://www.cwi.nl/~zhang/soap/" at "http://www.cwi.nl/~zhang/soap/soap.xq";

for $i in ("hello", "nihao")
    return soap:concatStr("localhost", $i, ", this message is sent back by the SOAP receiver.")
