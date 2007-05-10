create table t2 (id int);

insert into t2 values(1);
insert into t2 values(2);
insert into t2 values(3);
insert into t2 values(4);
insert into t2 values(5);
insert into t2 values(6);
insert into t2 values(7);
insert into t2 values(8);
insert into t2 values(9);


create table t1 as select * from t2 order by id asc with data;
select * from t1;

drop table t1;
drop table t2;
