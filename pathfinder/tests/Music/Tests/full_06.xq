(: q6 'SELECT * FROM Assets ORDER BY TrackNr' :)
for $asset in doc("music.xml")//Asset 
order by zero-or-one($asset/TrackNr)
return $asset
