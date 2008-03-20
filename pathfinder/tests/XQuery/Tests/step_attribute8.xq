let $a := doc("book.xml")//book[3] return $a/(attribute::text() | attribute::element(year) | attribute::document-node() | attribute::processing-instruction())
