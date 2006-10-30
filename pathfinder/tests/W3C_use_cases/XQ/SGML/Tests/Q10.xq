<result>
  {
    let $x := exactly-one(doc("sgml.xml")//xref[@xrefid = "top4"]),
        $t := doc("sgml.xml")//title[. << $x]
    return $t[last()]
  }
</result>
