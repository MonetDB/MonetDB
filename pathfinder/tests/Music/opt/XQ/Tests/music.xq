module namespace music = "http://www.cwi.nl/~boncz/music/opt/";

(: 'SELECT * FROM Assets' :)
declare function music:Asset($db    as xs:string, 
                             $limit as xs:integer) as xs:anyNode*
{
  subsequence(doc($db)/descendant::Asset, 1E0, $limit)
};


(: q2 'SELECT * FROM Assets WHERE TrackNr=1' :)
(: HACK: replaced value test by element name test :)
declare function music:AssetByTrackNr1($db      as xs:string, 
				       $limit   as xs:integer) as xs:anyNode*
{
  subsequence(doc($db)/descendant::TrackNr1, 1E0, $limit)/parent::*
};


(: q3 'SELECT * FROM Assets WHERE TrackNr=99999999' :)
(: HACK: replaced value test by element name test :)
declare function music:AssetByTrackNr9999999($db      as xs:string, 
                                             $limit   as xs:integer) as xs:anyNode*
{
  subsequence(doc($db)/descendant::TrackNr9999999, 1E0, $limit)/parent::*
};


(: q4 'SELECT alb.Title, ass.TrackName FROM Assets as ass, Albums as alb WHERE ass.AlbumId=alb.AlbumId' :)
declare function music:AlbumAsset($db    as xs:string, 
                                  $limit as xs:integer) as xs:anyNode*
{
  let $albums := subsequence(doc($db)/descendant::Asset, 1E0, $limit)/parent::*
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
(: HACK: replaced value test by element name test :)
declare function music:AlbumAssetByTrackNr1($db      as xs:string, 
                                            $limit   as xs:integer) as xs:anyNode*
{
  let $albums := subsequence(doc($db)/descendant::TrackNr1, 1E0, $limit)/ancestor::Album
  return
  subsequence(for $album in $albums
                for $asset in $album/child::Asset
                where $asset/child::TrackNr/text() = "1"
                return
<Result>
  <Album> { $album/child::Title/text() } </Album>
  <Track> { $asset/child::TrackName/text() } </Track>
</Result>, 1E0, $limit)
};



(: q6 'SELECT * FROM Assets ORDER BY TrackNr' :)
(: HACK: exploit tracknames being ordered in the index and omit sort :)
declare function music:AssetSort($db    as xs:string, 
                                   $limit as xs:integer) as xs:anyNode*
{
  let $doc := doc($db)
  let $result := $doc/descendant::TrackNr1/parent::*
  return
    if (count($result) >= $limit) then
      subsequence($result, 1E0, $limit) 
    else 
      subsequence(for $asset in $doc/descendant::Asset
                  order by zero-or-one($asset/child::TrackNr/text())
                  return $asset, 1E0, $limit)
};
