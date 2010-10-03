for $x in doc("ID.1207096.book.xml")/bib/book
order by zero-or-one($x/price)
return $x/price 
