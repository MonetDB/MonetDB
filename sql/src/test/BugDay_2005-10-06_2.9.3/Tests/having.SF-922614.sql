create table t(i int);
insert into t values(1);
insert into t values(2);
select * from t where i= (select max(i) from t);
select * from t having i= max(i);
