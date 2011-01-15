select
/*
(select count(*)
 from   attx p1068
 where	p1068.tblid = 1068
 and	cast(p1068.tail as real) >= 100000),
(select count(*)
 from   attx p1068
 where	p1068.tblid = 1068
 and	cast(p1068.tail as real) < 100000
 and	cast(p1068.tail as real) >= 30000),
(select count(*)
 from   attx p1068
 where	p1068.tblid = 1068
 and	cast(p1068.tail as real) < 30000)
,
*/
(select count(*)
 from   hidx p1067
	left outer join 
	attx p1068 
	on p1067.tail = p1068.head
 where	p1067.tblid = 1067
 and	p1068.tblid = 1068
 and	p1068.tail is null);
