<html>
<body>
<h1>Slashdot.org News</h1>
<ul>
{
for $t in doc("http://slashdot.org/rss/index.rss")//*
where name($t) = 'item'
return 
 element li {
   element a {
     attribute href { for $s in $t/*
                      where name($s) = "link"
		      return text { $s } },
     text { for $r in $t/*
            where name($r) = "title"
	    return text { $r } }
   }
 }
}
</ul>
</body>
</html>

