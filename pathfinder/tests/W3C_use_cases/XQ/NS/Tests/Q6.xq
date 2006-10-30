declare namespace ma = "http://www.example.com/AuctionWatch";
declare namespace anyzone = "http://www.example.com/auctioneers#anyzone";

<Q6 xmlns:ma="http://www.example.com/AuctionWatch">
  {
    doc("auction.xml")//ma:Auction[@anyzone:ID]/ma:Schedule/ma:Close
  }
</Q6>
