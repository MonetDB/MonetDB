<html>
<body>
<h1>Slashdot.org News</h1>
<ol>
{
for $t in doc("http://slashdot.org/rss/index.rss")//*:item
return 
 element li {
   element a {
     attribute href { $t/*:link/text() },
     text { $t/*:title }
   }
 }
}
</ol>
</body>
</html>

