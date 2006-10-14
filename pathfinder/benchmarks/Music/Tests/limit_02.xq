(: q2 'SELECT * FROM Assets WHERE TrackNr=1' :)
subsequence(
	for $trackno in doc("music.xml")//TrackNr
        where $trackno = 1
        return $trackno
		, 1E0, 100E0)/parent::Asset
