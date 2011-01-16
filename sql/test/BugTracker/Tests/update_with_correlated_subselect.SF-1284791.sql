create table t1284791b (id2 int, val2 varchar(255));
create table t1284791a (id1 int, val1 varchar(255));
insert into t1284791a values (1,'1');
insert into t1284791b values (1,'2');
update t1284791a set val1 = (select val2 from t1284791b where id1 = id2) where id1 in (select id2 from t1284791b);
select * from t1284791a;
select * from t1284791b;

drop table t1284791a;
drop table t1284791b;
