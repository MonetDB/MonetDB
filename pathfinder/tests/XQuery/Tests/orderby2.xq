for $a in doc("book.xml")//book order by $a/title return ($a//last/text(), data($a/@year))
