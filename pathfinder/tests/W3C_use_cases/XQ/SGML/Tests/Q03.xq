<result>
  {
    for $c in doc("sgml.xml")//chapter
    where empty($c/intro)
    return $c/section/intro/para
  }
</result>
