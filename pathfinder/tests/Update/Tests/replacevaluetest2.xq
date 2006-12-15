(: test whether the order of operations is done correctly :)

(do insert <foo attr="value"/> as first into exactly-one(doc("replacevaluetest2.xml")/document),
 do replace value of exactly-one(doc("replacevaluetest2.xml")/document) with "content")
