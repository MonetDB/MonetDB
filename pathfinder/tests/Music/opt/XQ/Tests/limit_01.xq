(: 'SELECT * FROM Assets LIMIT 100' :)
import module namespace music = "http://www.cwi.nl/~boncz/music/opt/" at "http://www.cwi.nl/~boncz/music/opt/music.xq";
music:Asset("music.xml", 100) 
