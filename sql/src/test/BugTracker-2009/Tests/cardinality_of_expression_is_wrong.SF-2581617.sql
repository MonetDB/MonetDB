create table t1 (id int, age int);

insert into t1 values(1, 1);
insert into t1 values(1, 1);
insert into t1 values(2, 1);
insert into t1 values(3, 1);
insert into t1 values(4, 1);

create view v2 as (select id, age from t1 group by id, age);

select id from v2 where id = 2;

drop view v2;

drop table t1;
