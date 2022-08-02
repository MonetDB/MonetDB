start transaction;
create table t3257 (i int, j int);
insert into t3257 values (0, 0), (1, 0), (2, 1), (3, 1);
select stddev_pop(i) from t3257;
select stddev_pop(i) from t3257 group by j;
select var_pop(i) from t3257;
select var_pop(i) from t3257 group by j;
rollback;
