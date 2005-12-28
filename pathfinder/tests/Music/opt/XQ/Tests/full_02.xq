(: q2 'SELECT * FROM Assets WHERE TrackNr=1' :)
import module namespace music = "http://www.cwi.nl/~boncz/music/opt/" at "http://www.cwi.nl/~boncz/music/opt/music.xq";
music:AssetByTrackNr1("music.xml", 1000000000) 
