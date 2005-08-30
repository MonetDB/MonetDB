 <bib> { 
   for $b in doc("http://monetdb.cwi.nl/XQuery/files/bib.xml")/bib/book 
   where $b/publisher = "Addison-Wesley" 
     and $b/@year > 1991 
   return 
     <book year="{ $b/@year }"> 
       { $b/title } 
     </book> 
 } </bib> 
