<Q1>
  {
    for $n in distinct-values(
                  for $i in (doc("auction.xml")//* | doc("auction.xml")//@*)
                  return namespace-uri($i) 
               )
    return  <ns>{$n}</ns>
  }
</Q1>
