(: q4 'SELECT alb.Title, ass.Title FROM Assets as ass, Albums as alb WHERE ass.AlbumId=alb.AlbumId LIMIT 100' :)
let $albums := subsequence(doc("music.xml")//Asset, 1E0, 100E0)/parent::Album
return
  subsequence(for $album in $albums
                for $asset in $album/Asset
                return
<Result>
  <Album> { $album/Title/text() } </Album>
  <Track> { $asset/TrackName/text() } </Track>
</Result>, 1E0, 100E0)
