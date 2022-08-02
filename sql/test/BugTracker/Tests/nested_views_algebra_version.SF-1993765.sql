create table t11993765(id int, age int);

create view v11993765 as select * from t11993765 where id = 0;

create view v21993765 as select * from v11993765 where age =1;

select * from v21993765 LIMIT 10;

drop view v21993765;
drop view v11993765;
drop table t11993765;
