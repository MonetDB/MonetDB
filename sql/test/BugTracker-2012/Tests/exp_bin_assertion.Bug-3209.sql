create table t (id int, x int, y int);
select * from t as t1, t as t2 where power((t1.x - t2.x),2) < power((t1.y -
		t2.y),2);
drop table t;
