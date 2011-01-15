select p1051.head, p1054.tail, count (p1051.tail)
from   hidx p1052, hidx p1053, attx p1054,
       attx p1051,
       attx p1252
where  p1051.tblid = 1051
and    p1051.head  = p1052.head
and    p1052.tblid = 1052
and    p1053.tblid = 1053
and    p1054.tblid = 1054
and    p1252.tblid = 1252
and    p1052.tail = p1053.head
and    p1053.tail = p1054.head
and    p1252.tail = p1051.tail
group by p1051.head, p1054.tail;
