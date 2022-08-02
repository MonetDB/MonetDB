create table t_0_1(id int, name varchar(1024), age int);
create table t_0_2(id int, age int);

create trigger test_0 after insert on t_0_1
	insert into t_0_2 select id,age from t_0_1;

insert into t_0_1 values(1, 'mo', 25);

select * from t_0_1;
select * from t_0_2;

create trigger test_1 after delete on t_0_1
	insert into t_0_1 values(3, 'mo', 27);

delete from t_0_1 where id = 1;

select * from t_0_1;
select * from t_0_2;

create trigger test_2 after update on t_0_1
	delete from t_0_2;

create trigger test_3 after delete on t_0_2
	insert into t_0_1 values(1, 'mo', 25);

update t_0_1 set name = 'monet' where id = 2;

select * from t_0_1;
select * from t_0_2;

drop trigger test_0;

drop trigger test_1;

drop trigger test_2;

drop trigger test_3;

drop table t_0_1;

drop table t_0_2;
