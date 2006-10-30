declare namespace xlink = "http://www.w3.org/1999/xlink";

<Q4 xmlns:xlink="http://www.w3.org/1999/xlink">
  {
    for $hr in doc("auction.xml")//@xlink:href
    return <ns>{ $hr }</ns>
  }
</Q4>
