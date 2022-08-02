--test the if the triggers action is accepted when the trigger event has BEFORE OR AFTER

--when the trigger event contains BEFORE the trigger action can only contain (DECLARE TABLE, DECLARE CURSOR,
--OPEN, CLOSE, FETCH, SELECT (for a single row), FREE LOCATOR, HOLD LOCATOR, CALL, RETURN and GET DIAGNOSTIC

--when the trigger event contains AFTER the trigger action can contain all the action define for the BEFORE
--plus INSERT, UPDATE, and DELETE

create table t1 (id int, name varchar(1024));

create trigger test1 after insert on t1
	insert into t1 values(0, 'after');


create trigger test2 before insert on t1
	insert into t1 values(0, 'before');

insert into t1(3,'monetdb');

select * from t1;

delete from t1 where id > -1;

drop trigger test1;
drop trigger test2;

create trigger test1 after insert on t1
	delete from t1 where id = 3;


create trigger test4 before insert on t1
	delete from t1 where id = 3;

insert into t1(3,'monetdb');

select * from t1;

delete from t1 where id > -1;

drop trigger test1;
drop trigger test2;

create trigger test1 after insert on t1
	update t1 set name = 'test1' where id = 3;


create trigger test4 before insert on t1
	update t1 set name = 'test2' where id = 3;

insert into t1(3,'monetdb');

select * from t1;

delete from t1 where id > -1;

drop trigger test1;
drop trigger test2;


--Cleanup
drop table t1;
