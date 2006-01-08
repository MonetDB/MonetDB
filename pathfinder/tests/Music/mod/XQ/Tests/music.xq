module namespace music = "http://www.cwi.nl/~boncz/music/mod/";

(: 'SELECT * FROM Assets' :)
declare function music:Asset($db    as xs:string, 
                             $limit as xs:integer) as xs:anyNode*
{
  subsequence(doc($db)/descendant::Asset, 1E0, $limit)
};


(: q2 'SELECT * FROM Assets WHERE TrackNr=1' :)
(: q3 'SELECT * FROM Assets WHERE TrackNr=99999999' :)
declare function music:AssetByTrackNr($db      as xs:string, 
                                      $tracknr as xs:integer, 
                                      $limit   as xs:integer) as xs:anyNode*
{
  subsequence(for $track in doc($db)/descendant::TrackNr
              where $track/text() = $tracknr
              return $track, 1E0, $limit)/parent::Asset
};


(: q4 'SELECT alb.Title, ass.TrackName FROM Assets as ass, Albums as alb WHERE ass.AlbumId=alb.AlbumId' :)
declare function music:AlbumAsset($db    as xs:string, 
                                  $limit as xs:integer) as xs:anyNode*
{
  let $albums := subsequence(doc($db)/descendant::Asset, 1E0, $limit)/parent::Album
  return 
  subsequence(for $album in $albums
                for $asset in $album/child::Asset
                return
<Result>
  <Album> { $album/child::Title/text() } </Album>
  <Track> { $asset/child::TrackName/text() } </Track>
</Result>, 1E0, $limit)
};


(: q5 'SELECT alb.Title, ass.TrackName FROM Assets as ass, Albums as alb WHERE ass.AlbumId=alb.AlbumId and ass.TrackNr=1' :)
declare function music:AlbumAssetByTrackNr($db      as xs:string, 
                                           $tracknr as xs:integer,
                                           $limit   as xs:integer) as xs:anyNode*
{
  let $albums := subsequence(doc($db)/descendant::Asset[./TrackNr/text() = $tracknr], 1E0, $limit)/parent::Album
  return 
  subsequence(for $album in $albums
                for $asset in $album/child::Asset
                where $asset/child::TrackNr/text() = $tracknr
                return
<Result>
  <Album> { $album/child::Title/text() } </Album>
  <Track> { $asset/child::TrackName/text() } </Track>
</Result>, 1E0, $limit)
};

(: q6 'SELECT * FROM Assets ORDER BY TrackNr' :)
declare function music:AssetSort($db    as xs:string, 
                                 $limit as xs:integer) as xs:anyNode*
{
  subsequence(for $asset in doc($db)/descendant::Asset
              order by zero-or-one($asset/child::TrackNr/text())
              return $asset, 1E0, $limit)
};
