(: q6 'SELECT * FROM Assets ORDER BY TrackNr' :)
for $asset in doc("music.xml")//Asset 
order by $asset/TrackNr
return $asset
