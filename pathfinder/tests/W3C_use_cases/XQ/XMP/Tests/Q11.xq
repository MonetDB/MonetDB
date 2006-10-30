<bib>
{
        for $b in doc("http://bstore1.example.com/bib.xml")//book[author]
        return
            <book>
                { $b/title }
                { $b/author }
            </book>
}
{
        for $b in doc("http://bstore1.example.com/bib.xml")//book[editor]
        return
          <reference>
            { $b/title }
            {$b/editor/affiliation}
          </reference>
}
</bib>
