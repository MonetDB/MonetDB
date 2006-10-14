(: q5 'SELECT alb.Title, ass.Title FROM Assets as ass, Albums as alb WHERE ass.AlbumId=alb.AlbumId and ass.TrackNr=1 LIMIT 100' :)
let $albums := subsequence(doc("music.xml")//Asset[./TrackNr = 1], 1E0, 100E0)/parent::Album
return
  subsequence(for $album in $albums
                for $asset in $album/Asset
                where $asset/TrackNr = 1 
                return
<Result>
  <Album> { $album/Title/text() } </Album>
  <Track> { $asset/TrackName/text() } </Track>
</Result>, 1E0, 100E0)
