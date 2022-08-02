create table t1 (i int);
create temp table tmp (like t1);
select * from tmp;
drop table tmp;

drop table t1;

