<result>
  {
    for $id in doc("sgml.xml")//xref/@xrefid
    return doc("sgml.xml")//topic[@topicid = $id]
  }
</result>
