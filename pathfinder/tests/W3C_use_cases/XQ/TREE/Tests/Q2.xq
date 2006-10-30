<figlist>
  {
    for $f in doc("book.xml")//figure
    return
        <figure>
            { $f/@* }
            { $f/title }
        </figure>
  }
</figlist>
