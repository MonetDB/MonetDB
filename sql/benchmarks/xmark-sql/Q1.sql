select p1054.tail
from   attx p1051,
       hidx p1052, hidx p1053, attx p1054
where  p1051.tblid = 1051
and    p1052.tblid = 1052
and    p1053.tblid = 1053
and    p1054.tblid = 1054
and    p1051.head = p1052.head
and    p1052.tail = p1053.head
and    p1053.tail = p1054.head
and    p1051.tail = 'person0';
