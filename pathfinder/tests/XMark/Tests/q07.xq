let $auction := doc("auctionG.xml") return
for $p in $auction/site
return
  count($p//description) + count($p//annotation) + count($p//emailaddress)

