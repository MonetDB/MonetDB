start transaction;
create table t3086 (d double);
insert into t3086 values (0.011);
select mod(0.011 + 180, 360);
select mod(d + 180, 360) from t3086;
rollback;
