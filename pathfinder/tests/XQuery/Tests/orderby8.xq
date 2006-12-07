let $auction := doc("auctionG.xml") return
for $b in $auction/site/open_auctions/open_auction
let $c := $b/bidder[1]/increase/text()
order by data(zero-or-one($c)) cast as xs:double? descending empty greatest
return if ($c) then data(zero-or-one($c)) cast as xs:double? else -1

