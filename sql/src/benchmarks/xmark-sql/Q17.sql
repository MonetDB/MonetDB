select names.head as id, names.tail as name
from   (select p1052.head, p1054.tail
        from   hidx p1052, hidx p1053, attx p1054
	where  p1052.tblid = 1052
	and    p1053.tblid = 1053
	and    p1054.tblid = 1054
	and    p1052.tail = p1053.head
	and    p1053.tail = p1054.head) as names
       left outer join
       (select * from hidx p1061 where p1061.tblid = 1061) as home
	on names.head = home.head
where   home.tail is null;
