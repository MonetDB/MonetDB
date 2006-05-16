select count(*) from tables;
create local temporary table t1(i int);
insert into t1 values(1);
insert into t1 values(2);
select * from t1;
drop table t1;
