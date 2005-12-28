(: q3 'SELECT * FROM Assets WHERE TrackNr=9999999' :)
subsequence(
	for $trackno in doc("music.xml")//TrackNr
        where $trackno = 9999999
        return $trackno
		, 1E0, 100E0)/parent::Asset
