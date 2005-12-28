(: q2 'SELECT * FROM Assets WHERE TrackNr=1' :)
import module namespace music = "http://www.cwi.nl/~boncz/music/mod/" at "http://www.cwi.nl/~boncz/music/mod/music.xq";
music:AssetByTrackNr("music.xml", 1, 1000000000) 
