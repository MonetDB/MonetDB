<result>
  {
    for $i in doc("sgml.xml")//intro/para[1]
    return
        <first_letter>{ substring(string($i), 1, 1) }</first_letter>
  }
</result>
