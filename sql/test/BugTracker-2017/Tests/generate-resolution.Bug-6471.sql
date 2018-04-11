create table t (i) as (select * from sys.generate_series(0, 10000000, 1));
drop table t;
