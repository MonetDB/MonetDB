<section_list>
  {
    for $s in doc("book.xml")//section
    let $f := $s/figure
    return
        <section title="{ $s/title/text() }" figcount="{ count($f) }"/>
  }
</section_list>
