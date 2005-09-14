(: doc("document_1.xml")/site/people/person[string-length(translate(concat(address/street,address/city,address/country,address/zipcode)," ","")) > 30] :)
doc("document_1.xml")/site/people/person[string-length(translate(concat(zero-or-one(address/street),zero-or-one(address/city),zero-or-one(address/country),zero-or-one(address/zipcode))," ","")) > 30]
