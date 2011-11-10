create table t ( i int, j int );
insert into t values (1,2),(2,3),(3,4),(4,1), (5,3);
insert into t values (1,2),(2,3),(3,4),(4,1), (5,3);
select * from t where i < 10 order by i asc, j  desc;
drop table t;
