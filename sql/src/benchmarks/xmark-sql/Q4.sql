select p1110.tail
from   hidx p1111a, hidx p1118a, attx p1119a,
       hidx p1111b, hidx p1118b, attx p1119b,
       hidx p1108, hidx p1109, attx p1110,
       hidx p1103
where  p1103.tblid = 1103
and    p1103.tail = p1111a.head
and    p1103.tail = p1111b.head
and    p1111a.tblid = 1111
and    p1118a.tblid = 1118
and    p1119a.tblid = 1119
and    p1111a.tail = p1118a.head
and    p1118a.tail = p1119a.head
and    p1111b.tblid = 1111
and    p1118b.tblid = 1118
and    p1119b.tblid = 1119
and    p1111b.tail = p1118b.head
and    p1118b.tail = p1119b.head
and    p1111a.head = p1111b.head
and    p1119a.tail = 'person18829'
and    p1119b.tail = 'person10487'
and    p1108.tblid = 1108
and    p1109.tblid = 1109
and    p1110.tblid = 1110
and    p1108.tail = p1109.head
and    p1109.tail = p1110.head
and    p1108.head = p1111a.head
and    p1111a.rank < p1111b.rank;
