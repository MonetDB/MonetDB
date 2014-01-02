-- query 3 

select increase_string_fst.tail, increase_string_lst.tail
from   X01103 auction,
       X01111 bidder_fst, 
       X01120 increase_fst, X01121 increase_cdata_fst, X01122 increase_string_fst,
       X01111 bidder_lst,
       X01120 increase_lst, X01121 increase_cdata_lst, X01122 increase_string_lst
where  auction.tail = bidder_fst.head
and    bidder_fst.tail = increase_fst.head
and    increase_fst.tail = increase_cdata_fst.head
and    increase_cdata_fst.tail = increase_string_fst.head
and    bidder_fst.rank = (select min(allbids.rank)
		       	  from X01111 allbids
			  where auction.tail = allbids.head)
and    auction.tail = bidder_lst.head
and    bidder_lst.tail = increase_lst.head
and    increase_lst.tail = increase_cdata_lst.head
and    increase_cdata_lst.tail = increase_string_lst.head
and    bidder_lst.rank = (select max(allbids.rank)
			  from X01111 allbids
			  where auction.tail = allbids.head)
and    2 * CAST(increase_string_fst.tail as real) < CAST(increase_string_lst.tail as real);

