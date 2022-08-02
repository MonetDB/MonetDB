create table test (id serial, kvks int);
select count(kvks) from 
	(select x2.t2 as fr, x1.t1 as t from  
		(select row_number() over (order by k1.kvks) as rn, k1.kvks as t1, k2.kvks as t2 from test as k1, test as k2 where (k1.id + 1) = k2.id AND (k1.kvks + 1000) < k2.kvks) as x1, 
		(select  row_number() over (order by k1.kvks) as rn, k1.kvks as t1, k2.kvks as t2 from test as k1, test as k2 where (k1.id + 1) = k2.id AND (k1.kvks + 1000) < k2.kvks) as x2 
	where (x2.rn + 1) = x1.rn) as y, test 
where kvks between fr and t group by kvks;

drop table test;
