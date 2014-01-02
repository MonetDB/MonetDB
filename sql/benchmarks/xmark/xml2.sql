-- query 2

select increase_string.tail 
from   X01111 bidder, X01120 increase, X01121 increase_cdata, X01122 increase_string,
       X01103 auction 
where  bidder.tail = increase.head
and    increase.tail = increase_cdata.head
and    increase_cdata.tail = increase_string.head
and    auction.tail = bidder.head
and    bidder.rank = (select min(allbids.rank)
		      from X01111 allbids
		      where auction.tail = allbids.head);

