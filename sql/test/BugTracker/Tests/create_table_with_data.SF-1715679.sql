create table t1715679b (id int);

insert into t1715679b values(1);
insert into t1715679b values(2);
insert into t1715679b values(3);
insert into t1715679b values(4);
insert into t1715679b values(5);
insert into t1715679b values(6);
insert into t1715679b values(7);
insert into t1715679b values(8);
insert into t1715679b values(9);


create table t1715679a as select * from t1715679b order by id asc with data;
select * from t1715679a;

drop table t1715679a;
drop table t1715679b;
