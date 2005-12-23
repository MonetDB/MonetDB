for $a in doc("book.xml")//book order by zero-or-one($a/title) return ($a//last/text(), data($a/@year))
