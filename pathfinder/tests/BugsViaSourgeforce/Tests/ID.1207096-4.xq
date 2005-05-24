for $x in doc("ID.1207096.book.xml")/bib/book
order by exactly-one($x/price/text()) cast as xs:decimal
return $x/price
