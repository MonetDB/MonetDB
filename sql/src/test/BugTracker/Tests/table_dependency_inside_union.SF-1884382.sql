create table t1 (id int);
create table t2 (id int);

create view v1 as (select * from t1
                        union
                   select * from t2);

drop table t1;
drop table t2;

drop view v1;

select * from t1;
drop table t1;
drop table t2;
