(: q4 'SELECT alb.Title, ass.Title FROM Assets as ass, Albums as alb WHERE ass.AlbumId=alb.AlbumId' :)
for $album in doc("music.xml")//Album
  for $asset in $album/Asset
  return 
<Result>
  <Album> { $album/Title/text() } </Album>
  <Track> { $asset/TrackName/text() } </Track>
</Result>
