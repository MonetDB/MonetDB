let $auction := doc("auctionG.xml") return
for $b in $auction/site/regions//item
let $k := $b/name/text()
return <item name="{$k}">{$b/location/text()}</item>

