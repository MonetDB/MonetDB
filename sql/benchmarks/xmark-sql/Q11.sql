select person.name, person.id, count(prices.id)
from   (select p1051.tail as id, p1054.tail as name, cast (p1068.tail as float) as income
	from   hidx p1052, hidx p1053, attx p1054,
               attx p1051,
               hidx p1067, attx p1068
	where  p1052.tblid = 1052
	and    p1053.tblid = 1053
	and    p1054.tblid = 1054
	and    p1052.tail = p1053.head
	and    p1053.tail = p1054.head
	and    p1051.tblid = 1051
	and    p1051.head = p1052.head
	and    p1068.tblid = 1068
	and    p1067.tblid = 1067
	and    p1067.tail  = p1068.head
	and    p1067.head  = p1051.head) as person
       left outer join
       (select p1107.head as id,cast (p1107.tail as float) as value
        from attx p1107
        where p1107.tblid = 1107) as prices
       on person.income > 5000 * prices.value
group by person.name, person.id;
