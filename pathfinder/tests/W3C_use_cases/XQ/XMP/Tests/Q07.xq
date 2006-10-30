<bib>
  {
    for $b in doc("bib.xml")//book
    where $b/publisher = "Addison-Wesley" and $b/@year > 1991
    order by exactly-one($b/title)
    return
        <book>
            { $b/@year }
            { $b/title }
        </book>
  }
</bib>
