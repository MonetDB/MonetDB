select p1122.tail as increase
from   attx p1122, hidx p1121, hidx p1120, hidx p1111
where  p1122.tblid = 1122
and    p1121.tblid = 1121
and    p1120.tblid = 1120
and    p1111.tblid = 1111
and    p1111.tail = p1120.head
and    p1120.tail = p1121.head
and    p1121.tail = p1122.head
and    p1111.rank = (select min(rank)
                     from   hidx p1111a
		     where  p1111a.tblid = 1111
		     and    p1111a.head = p1111.head);
