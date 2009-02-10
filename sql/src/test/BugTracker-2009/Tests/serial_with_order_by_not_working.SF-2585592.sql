
create table t1_2585592 (num int, age int);

create table t2_2585592 (id serial, num int, age int);

insert into t1_2585592 values(1,3);
insert into t1_2585592 values(1,4);
insert into t1_2585592 values(3,3);
insert into t1_2585592 values(6,3);
insert into t1_2585592 values(2,3);
insert into t1_2585592 values(2,2);
insert into t1_2585592 values(5,1);
insert into t1_2585592 values(1,1);

insert into t2_2585592(num, age) select num, age from t1_2585592 order by num, age;

select * from t2_2585592;

drop table t1_2585592;
drop table t2_2585592; 
