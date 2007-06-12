create table t1 (id int, name varchar(1024), PRIMARY KEY(id));

insert into t1 values(1, 'monetdb');
insert into t1 values(2, 'mon');

update t1 set id = 10 where id =1;
update t1 set id = 10 where id =2;
update t1 set id = 12 where id =3;

select * from t1;
drop table t1;
