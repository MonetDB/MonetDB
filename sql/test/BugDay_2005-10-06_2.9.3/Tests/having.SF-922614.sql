create table t10(i int);
insert into t10 values(1);
insert into t10 values(2);
select * from t10 where i= (select max(i) from t10);
select * from t10 having i= max(i);
