(: q6 'SELECT * FROM Assets ORDER BY Title LIMIT 100' :)
subsequence(
	for $asset in doc("music.xml")//Asset 
	order by $asset/TrackNr
	return $asset
		, 1E0, 100E0)
