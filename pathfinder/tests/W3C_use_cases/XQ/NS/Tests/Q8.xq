declare namespace ma = "http://www.example.com/AuctionWatch";

<Q8 xmlns:ma="http://www.example.com/AuctionWatch"
    xmlns:eachbay="http://www.example.com/auctioneers#eachbay" 
    xmlns:xlink="http://www.w3.org/1999/xlink">
  {
    for $s in doc("auction.xml")//ma:Trading_Partners/(ma:Seller | ma:High_Bidder)
    where $s/*:NegativeComments = 0
    return $s
  }
</Q8>
