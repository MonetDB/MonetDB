create table t1 (c1 integer);
insert into t1 values (1);
select count(*) / (select count(*) from t1) as c2 from t1;
select sum(c2) from (select count(*) / (select count(*) from t1) as c2 from t1) as t;
drop table t1;
