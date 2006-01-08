(: q2 'SELECT * FROM Assets WHERE TrackNr=1' :)
(for $trackno in doc("music.xml")//TrackNr
 where $trackno = 1
 return $trackno)/parent::Asset
