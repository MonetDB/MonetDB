doc("document_1.xml")/site/people/person[string-length(translate(concat(concat(address/street,address/city),concat(address/country,address/zipcode))," ","")) > 30]
