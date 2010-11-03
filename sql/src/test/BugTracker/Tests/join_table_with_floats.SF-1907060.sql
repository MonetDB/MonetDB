create table t1907060_2 (id float);
create table t1907060_1 (age float);

select age from t1907060_1, t1907060_2 where id < 0.0 limit 10;

drop table t1907060_1;
drop table t1907060_2;

