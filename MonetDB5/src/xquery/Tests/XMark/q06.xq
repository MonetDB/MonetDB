let $auction := doc("auctionG.xml") return for $b in $auction//site/regions return count($b//item)

