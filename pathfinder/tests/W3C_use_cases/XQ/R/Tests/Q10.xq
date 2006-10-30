<result>
 {
    for $highbid in doc("bids.xml")//bid_tuple,
        $user in doc("users.xml")//user_tuple
    where $user/userid = $highbid/userid 
      and $highbid/bid = max(doc("bids.xml")//bid_tuple[itemno=$highbid/itemno]/bid)
    order by $highbid/itemno
    return
        <high_bid>
            { $highbid/itemno }
            { $highbid/bid }
            <bidder>{ $user/name/text() }</bidder>
        </high_bid>
  }
</result>
