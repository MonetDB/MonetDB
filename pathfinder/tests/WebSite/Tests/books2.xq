declare function toc($book-or-section as element()) as element()*
{
    for $section in $book-or-section/section
    return
      <section>
         { $section/@* , $section/title , toc($section) }                 
      </section>
};
<toc>
   {
     for $s in doc("http://monetdb.cwi.nl/XQuery/files/book.xml")/book return toc($s)
   }
</toc> 

