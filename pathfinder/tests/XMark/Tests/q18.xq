let $auction := doc("auctionG.xml") return
for $i in $auction/site/open_auctions/open_auction
return local:convert($i/reserve)

