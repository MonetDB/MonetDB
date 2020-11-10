start transaction;
create table t3065(c integer);
insert into t3065 values(1),(2);
delete from t3065 where (1=1 and c=2);
select * from t3065;
rollback;
