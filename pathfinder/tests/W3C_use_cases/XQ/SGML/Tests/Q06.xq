<result>
  {
    for $s in doc("sgml.xml")//section/@shorttitle
    return <stitle>{ $s }</stitle>
  }
</result>
