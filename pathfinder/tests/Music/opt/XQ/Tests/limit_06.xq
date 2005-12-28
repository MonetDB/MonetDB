(: q6 'SELECT * FROM Assets ORDER BY Title LIMIT 100' :)
import module namespace music = "http://www.cwi.nl/~boncz/music/opt/" at "http://www.cwi.nl/~boncz/music/opt/music.xq";
music:AssetSort("music.xml", 100)
