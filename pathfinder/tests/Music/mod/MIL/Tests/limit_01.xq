(: 'SELECT * FROM Assets LIMIT 100' :)
import module namespace music = "http://www.cwi.nl/~boncz/music/mod/" at "http://www.cwi.nl/~boncz/music/mod/music.mil";
music:Asset("music.xml", 100) 
