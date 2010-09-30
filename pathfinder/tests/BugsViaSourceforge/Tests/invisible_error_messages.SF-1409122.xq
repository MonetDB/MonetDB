for    $b in doc("auction.xml")/site/open_auctions/open_auction
where  $b/bidder/personref[@person="person18829"] <<
       $b/bidder/personref[@person="person10487"]
       return <history> {$b/reserve/text()} </history>
