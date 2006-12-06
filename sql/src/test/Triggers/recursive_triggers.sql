create table t1(id int, name varchar(1024), age int);
create table t2(id int, age int);

create trigger test_0 after insert on t1
	insert into t1 values(1, 'monetdb', 24);

insert into t1 values(1, 'mo', 25);

select * from t1;
select * from t2;

create trigger test_1 after insert on t1
	insert into t2 select id,age from t1;

insert into t1 values(2, 'mo', 26);

select * from t1;
select * from t2;

create trigger test_2 before insert on t1
	insert into t2 values(1,23);

insert into t1 values(3, 'mo', 27);

select * from t1;
select * from t2;

create trigger test_3 after delete on t1
	delete from t1 where id =3;

delete from t1 where id = 1;

select * from t1;
select * from t2;

create trigger test_4 after update on t1
	update t1
	set age = 27
	where id = 2;

update t1 set name = 'monet' where id = 2;

select * from t1;
select * from t2;

drop trigger test_0;

drop trigger test_1;

drop trigger test_2;

drop trigger test_3;

drop trigger test_4;

drop table t1;

drop table t2;
