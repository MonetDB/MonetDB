create table t2 (id float);
create table t1 (age float);

select age from t1, t2 where id < 0.0 limit 10;

drop table t1;
drop table t2;

