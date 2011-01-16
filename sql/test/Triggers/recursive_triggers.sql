create table t1(id int, name varchar(1024), age int);
create table t2(id int, age int);

--the trigger calls itself
create trigger test_0 after insert on t1
	insert into t1 values(3, 'mo', 27);

drop trigger test_0;

--recursivity of 2 levels
create trigger test_0 after insert on t1
	insert into t2 select id,age from t1;
create trigger test_1 after insert on t2
	insert into t1 values(3, 'mo', 27);


drop trigger test_0;
drop trigger test_1;

--recursivity of n levels
create trigger test_0 after insert on t1
	insert into t2 select id,age from t1;

create trigger test_1 after insert on t2
	delete from t2;

create trigger test_2 after delete on t2
	insert into t1 values(3, 'mo', 27);


drop trigger test_0;
drop trigger test_1;
drop trigger test_2;

--recursivity with procedure calls
create PROCEDURE p1(id int, age int)
BEGIN
	insert into t2 values(id, age);
END;

create PROCEDURE p1()
BEGIN
	declare id int, age int;
	set id = 1;
	set age = 23;
	call p1(id, age);
END;

create trigger test_0 after insert on t2
BEGIN ATOMIC
	insert into t1 values(1, 'monetdb', 24);
	call p1();
END;

--insert into t2 values(0, 24);

drop trigger test_0;

drop ALL procedure p1;

drop table t1;

drop table t2;
