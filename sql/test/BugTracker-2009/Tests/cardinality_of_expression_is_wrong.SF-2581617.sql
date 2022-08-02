create table t1_2581617 (id int, age int);

insert into t1_2581617 values(1, 1);
insert into t1_2581617 values(1, 1);
insert into t1_2581617 values(2, 1);
insert into t1_2581617 values(3, 1);
insert into t1_2581617 values(4, 1);

create view v2_2581617 as (select id, age from t1_2581617 group by id, age);

select id from v2_2581617 where id = 2;

drop view v2_2581617;

drop table t1_2581617;
