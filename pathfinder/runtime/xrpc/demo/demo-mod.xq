module namespace df="demo-functions";

declare function df:auctionsByPerson(
          $doc as xs:string,
          $pid as xs:string) as node()*
{
  for $ca in doc($doc)//closed_auction[./buyer/@person=$pid]
  return
    element closed_auction {
      attribute buyer {$ca/buyer/@person},
      $ca/price
    }
};

