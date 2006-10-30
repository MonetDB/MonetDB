<bib>
{
    for $book1 in doc("http://bstore1.example.com/bib.xml")//book,
        $book2 in doc("http://bstore1.example.com/bib.xml")//book
    let $aut1 := for $a in $book1/author 
                 order by $a/last, $a/first
                 return $a
    let $aut2 := for $a in $book2/author 
                 order by $a/last, $a/first
                 return $a
    where $book1 << $book2
    and not($book1/title = $book2/title)
    and deep-equal($aut1, $aut2) 
    return
        <book-pair>
            { $book1/title }
            { $book2/title }
        </book-pair>
}
</bib>
