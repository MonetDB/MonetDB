create table t1703497a (id int, name varchar(1024), PRIMARY KEY(id));
create table t1703497b (id int, age int, FOREIGN KEY(id) REFERENCES t1703497a(id));
insert into t1703497a values(3, 'monb');
insert into t1703497b values(3, 25);
update t1703497b set id = 10 where id =3;

drop table t1703497b;
drop table t1703497a;
