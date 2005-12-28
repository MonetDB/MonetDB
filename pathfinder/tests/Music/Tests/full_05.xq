(: q5 'SELECT alb.Title, ass.Title FROM Assets as ass, Albums as alb WHERE ass.AlbumId=alb.AlbumId and ass.TrackNr=1' :)
for $album in doc("music.xml")//Album
  for $asset in $album/Asset
  where $asset/TrackNr = 1
  return 
<Result>
  <Album> { $album/Title/text() } </Album>
  <Track> { $asset/TrackName/text() } </Track>
</Result>
