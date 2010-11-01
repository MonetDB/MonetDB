create table t1733712a (id int, name varchar(1024), PRIMARY KEY(id));

insert into t1733712a values(1, 'monetdb');
insert into t1733712a values(2, 'mon');

update t1733712a set id = 10 where id =1;
update t1733712a set id = 10 where id =2;
update t1733712a set id = 12 where id =3;

select * from t1733712a;
drop table t1733712a;
