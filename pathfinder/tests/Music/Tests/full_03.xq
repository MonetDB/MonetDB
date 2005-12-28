(: q3 'SELECT * FROM Assets WHERE TrackNr=9999999' :)
(for $trackno in doc("music.xml")//TrackNr
 where $trackno = 9999999
 return $trackno)/parent::Asset
