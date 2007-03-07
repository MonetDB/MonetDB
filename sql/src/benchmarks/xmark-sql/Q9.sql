select p1051.head, p1054.tail, p484.tail
from   hidx p1052, hidx p1053, attx p1054, attx p1051,
       hidx p1251, attx p1252, hidx p1253, attx p1254,
       attx p475, hidx p482, hidx p483, attx p484
where  p1051.tblid = 1051
and    p1052.tblid = 1052
and    p1053.tblid = 1053
and    p1054.tblid = 1054
and    p1251.tblid = 1251
and    p1252.tblid = 1252
and    p1253.tblid = 1253
and    p1254.tblid = 1254
and    p475.tblid = 475
and    p482.tblid = 482
and    p483.tblid = 483
and    p484.tblid = 484
and    p1051.head = p1052.head
and    p1052.tail = p1053.head
and    p1053.tail = p1054.head
and    p1251.tail = p1252.head
and    p1253.tail = p1254.head
and    p1251.head = p1253.head
and    p475.head = p482.head
and    p482.tail = p483.head
and    p483.tail = p484.head
and    p1252.tail = p1051.tail
and    p475.tail = p1254.tail;
