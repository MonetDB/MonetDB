select p1122f.tail as first, p1122l.tail as last
from   hidx p1103f, hidx p1111f, hidx p1120f, hidx p1121f, attx p1122f,
       hidx p1103l, hidx p1111l, hidx p1120l, hidx p1121l, attx p1122l
where  p1103f.tblid = 1103
and    p1103l.tblid = 1103
and    p1111f.tblid = 1111
and    p1120f.tblid = 1120
and    p1121f.tblid = 1121
and    p1122f.tblid = 1122
and    p1111l.tblid = 1111
and    p1120l.tblid = 1120
and    p1121l.tblid = 1121
and    p1122l.tblid = 1122
and    p1103f.tail  = p1103l.tail
and    p1103f.tail  = p1111f.head
and    p1111f.tail  = p1120f.head
and    p1120f.tail  = p1121f.head
and    p1121f.tail  = p1122f.head
and    p1103l.tail  = p1111l.head
and    p1111l.tail  = p1120l.head
and    p1120l.tail  = p1121l.head
and    p1121l.tail  = p1122l.head
and    p1111f.rank  = (select min(rank)
		       from   hidx p1111a
		       where  p1111a.tblid = 1111
		       and    p1111a.head = p1111f.head)
and    p1111l.rank  = (select max(rank)
		       from   hidx p1111b
		       where  p1111b.tblid = 1111
		       and    p1111b.head = p1111l.head)
and    2 * cast(p1122f.tail as float) < cast(p1122l.tail as float);
