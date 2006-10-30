<result>
  {
    for $seller in doc("users.xml")//user_tuple,
        $buyer in  doc("users.xml")//user_tuple,
        $item in  doc("items.xml")//item_tuple,
        $highbid in  doc("bids.xml")//bid_tuple
    where $seller/name = "Tom Jones"
      and $seller/userid  = $item/offered_by
      and contains($item/description , "Bicycle")
      and $item/itemno  = $highbid/itemno
      and $highbid/userid  = $buyer/userid
      and $highbid/bid = max(
                              doc("bids.xml")//bid_tuple
                                [itemno = $item/itemno]/bid
                         )
    order by ($item/itemno)
    return
        <jones_bike>
            { $item/itemno }
            { $item/description }
            <high_bid>{ $highbid/bid }</high_bid>
            <high_bidder>{ $buyer/name }</high_bidder>
        </jones_bike>
  }
</result>
