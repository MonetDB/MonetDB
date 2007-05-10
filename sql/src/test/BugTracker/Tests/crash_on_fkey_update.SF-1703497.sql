create table t1 (id int, name varchar(1024), PRIMARY KEY(id));
create table t2 (id int, age int, FOREIGN KEY(id) REFERENCES t1(id));
insert into t1 values(3, 'monb');
insert into t2 values(3, 25);
update t2 set id = 10 where id =3;

drop table t2;
drop table t1;
