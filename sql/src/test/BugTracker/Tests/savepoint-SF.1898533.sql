create table t1(i int);
create table t2(i int);
start transaction;
insert into t1 values(1);
savepoint s1;
insert into t2 values(1);
commit;
select * from t1;
select * from t2;
start transaction;
select * from t1;
select * from t2;
commit;
select * from t1;
select * from t2;

drop table t1;
drop table t2;
