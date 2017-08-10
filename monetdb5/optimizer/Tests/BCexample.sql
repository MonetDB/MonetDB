create table t(i int);
insert into t values(1),(2),(3),(4);

explain select 100+i from t where i<2;

drop table t;
