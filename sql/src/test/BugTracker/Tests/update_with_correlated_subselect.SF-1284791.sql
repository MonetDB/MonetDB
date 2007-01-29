create table t2 (id2 int, val2 varchar(255));
create table t1 (id1 int, val1 varchar(255));
insert into t1 values (1,'1');
insert into t2 values (1,'2');
update t1 set val1 = (select val2 from t2 where id1 = id2) where id1 in (select id2 from t2);
select * from t1;
select * from t2;

drop table t1;
drop table t2;
