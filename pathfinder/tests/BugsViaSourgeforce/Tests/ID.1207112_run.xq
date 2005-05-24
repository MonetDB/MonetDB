for $x at $i in doc("book.xml")/bib/book[price<=49.95]
return <li>{$i}. {data($x/title)}. {data($x/price)}</li>
