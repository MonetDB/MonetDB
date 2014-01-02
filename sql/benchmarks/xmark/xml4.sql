-- query 4

select reserve_string.tail
from   X01103 auction, X01108 reserve, X01109 reserve_cdata, X01110 reserve_string,
       X01111 bidder_one, X01118 personref_one, X01119 personid_one,
       X01111 bidder_two, X01118 personref_two, X01119 personid_two
where  auction.tail = reserve.head
and    reserve.tail = reserve_cdata.head
and    reserve_cdata.tail = reserve_string.head
and    auction.tail = bidder_one.head
and    bidder_one.tail = personref_one.head
and    personref_one.tail = personid_one.head
and    personid_one.tail = 'person18829'
and    auction.tail = bidder_two.head
and    bidder_two.tail = personref_two.head
and    personref_two.tail = personid_two.head
and    personid_two.tail = 'person10487'
and    bidder_one.rank < bidder_two.rank;

