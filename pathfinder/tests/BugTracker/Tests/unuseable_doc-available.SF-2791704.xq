if (doc-available("auctionG.xml")) then exists(doc("auctionG.xml")) else 42
<>
if (doc-available("auctionG.xml")) then exists(doc("not-existent.xml")) else 42
<>
if (doc-available("not-existent.xml")) then exists(doc("not-existent.xml")) else 42
<>
