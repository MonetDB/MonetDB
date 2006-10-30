let $allbikes := doc("items.xml")//item_tuple
                    [contains(zero-or-one(description), "Bicycle") 
                     or contains(zero-or-one(description), "Tricycle")]
let $bikebids := doc("bids.xml")//bid_tuple[itemno = $allbikes/itemno]
return
    <high_bid>
      { 
        max($bikebids/bid) 
      }
    </high_bid>
