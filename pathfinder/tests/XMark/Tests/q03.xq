let $auction := doc("auctionG.xml") return
for $b in $auction/site/open_auctions/open_auction
where exactly-one($b/bidder[1]/increase/text()) * 2 <= $b/bidder[last()]/increase/text()
return
  <increase
  first="{$b/bidder[1]/increase/text()}"
  last="{$b/bidder[last()]/increase/text()}"/>

