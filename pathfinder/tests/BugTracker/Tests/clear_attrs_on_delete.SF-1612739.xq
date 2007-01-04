for $i in (1,2,3,4,5,6,7,8,9,10)
  return do insert <foo attr1="value1" attr2="value2" attr3="value3"/> as first into exactly-one(doc("clear_attrs_on_delete.SF-1612739.xml")/document/test)
