(: q2 'SELECT * FROM Assets WHERE TrackNr=1 LIMIT 100' :)
import module namespace music = "http://www.cwi.nl/~boncz/music/mod/" at "http://www.cwi.nl/~boncz/music/mod/music.mil";
music:AssetByTrackNr("music.xml", 1, 100) 
