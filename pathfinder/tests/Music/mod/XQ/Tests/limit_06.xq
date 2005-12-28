(: q6 'SELECT * FROM Assets ORDER BY Title LIMIT 100' :)
import module namespace music = "http://www.cwi.nl/~boncz/music/mod/" at "http://www.cwi.nl/~boncz/music/mod/music.xq";
music:AssetSort("music.xml", 100)
