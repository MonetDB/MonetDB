<results>
  {
    for $t in doc("books.xml")//(chapter | section)/title
    where contains(zero-or-one($t/text()), "XML")
    return $t
  }
</results>
