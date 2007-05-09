set optimizer='off';
create table t1(id int, name varchar(1024), age int);
create table t2(id int, age int);

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

insert into t2 values(0, 24);

DROP table t2 CASCADE;
DROP Table t1;
