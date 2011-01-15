create table t1 (id int, name varchar(1024), PRIMARY KEY(id));

create table t2 (id int DEFAULT 3, age int, PRIMARY KEY (id), FOREIGN KEY(id) REFERENCES t1(id) ON DELETE SET DEFAULT ON UPDATE SET NULL);

create table t3 (id int DEFAULT 3, num int, FOREIGN KEY(id) REFERENCES t2(id) ON DELETE SET DEFAULT ON UPDATE SET NULL);



insert into t1 values(1, 'monetdb');
insert into t1 values(2, 'mon');
insert into t1 values(3, 'monb');
insert into t1 values(4, 'motdb');
insert into t1 values(5, 'mob');
insert into t1 values(6, 'moetdb');
insert into t1 values(7, 'mo');

insert into t2 values(1, 23);
insert into t2 values(2, 24);
insert into t2 values(3, 25);
insert into t2 values(4, 26);
insert into t2 values(5, 27);


insert into t3 values(3, 5);
insert into t3 values(3, 5);
insert into t3 values(4, 6);
insert into t3 values(5, 7);

delete from t1 where id = 4;

select * from t1;
select * from t2;
select * from t3;

update t1 set id = 12 where id =3;

select * from t1;
select * from t2;
select * from t3;

drop table t3;
drop table t2;
drop table t1;

