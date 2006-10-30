<result>
  {
    for $i in doc("items.xml")//item_tuple
    where empty(doc("bids.xml")//bid_tuple[itemno = $i/itemno])
    return
        <no_bid_item>
            { $i/itemno }
            { $i/description }
        </no_bid_item>
  }
</result>
