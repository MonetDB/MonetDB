create table t1(v0 int, v1 char(1));
insert into t1 values (1,'a'),(2,'b'),(3,'c');
create ordered index index_t1_v1 on t1(v1);

create table t2(v1 char(1));
insert into t2 values ('a');

create temp table t3 as
(select t1.v0 from t1,t2 where trim(t2.v1)=t1.v1)
on commit preserve rows;

drop index index_t1_v1;
drop table t1;
drop table t2;
drop table t3;
