create table t1 (a int);
select * from t1;
select * from t2;
select * from t1 where a in (select b from t2);
drop table t1;
