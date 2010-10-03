let $bib := doc("bib.xml")/bib return
<authlist>
 {
   for $a in fn:distinct-values($bib/book/author)
   order by $a
   return
     <author>
        <name> {$a} </name>
        <books>
          {
            for $b in $bib/book[author = $a]
            order by zero-or-one($b/title)
            return $b/title 
          }
        </books>
     </author>
 }
</authlist>
