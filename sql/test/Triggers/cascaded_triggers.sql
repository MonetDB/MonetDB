create table t1(id int, name varchar(1024), age int);
create table t2(id int, age int);

create trigger test_0 after insert on t1
	insert into t2 select id,age from t1;

insert into t1 values(1, 'mo', 25);

select * from t1;
select * from t2;

create trigger test_1 after delete on t1
	insert into t1 values(3, 'mo', 27);

delete from t1 where id = 1;

select * from t1;
select * from t2;

create trigger test_2 after update on t1
	delete from t2;

create trigger test_3 after delete on t2
	insert into t1 values(1, 'mo', 25);

update t1 set name = 'monet' where id = 2;

select * from t1;
select * from t2;

drop trigger test_0;

drop trigger test_1;

drop trigger test_2;

drop trigger test_3;

drop table t1;

drop table t2;
