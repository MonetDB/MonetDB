-- disable parallelism (mitosis & dataflow) to avoid ambiguous results 
set optimizer='sequential_pipe';
create table t2564 (c1 int, c2 int, c3 int);
insert into t2564 values (3,1,2);
insert into t2564 values (1,2,1);
insert into t2564 values (2,3,3);
select * from t2564;
--explain select count(*) from t2564 group by c1, c2, c3;
select count(*) from t2564 group by c1, c2, c3;
drop table t2564;
