start transaction;

create table t(a real, b real);
insert into t values (null, 1), (2, 1);
select * from t;
select sum(a) from t;
select sum(a * b) from t;
select sum(a), sum(a * b) from t;

rollback;
