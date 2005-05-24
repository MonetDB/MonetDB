for $x in doc("ID.1207090.book.xml")/bib/book           
where $x/price <= "65.95"
return $x/price
