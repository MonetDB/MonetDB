<frequent_bidder>
  {
    for $u in doc("users.xml")//user_tuple
    where 
      every $item in doc("items.xml")//item_tuple satisfies 
        some $b in doc("bids.xml")//bid_tuple satisfies 
          ($item/itemno = $b/itemno and $u/userid = $b/userid)
    return
        $u/name
  }
</frequent_bidder>
