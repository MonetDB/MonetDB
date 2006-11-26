--this test only tests the sintax
--the semantic should also be tested after the syntax test

create table t1 (id int, name varchar(1024));

--test when the trigger event is UPDATE
insert into t1 values(10, 'monetdb');
insert into t1 values(20, 'monet');


create trigger test1
	after update on t1 referencing new row as new_row
	for each row insert into t1 values(0, 'update_new_row');

create trigger test2
	after update on t1 referencing new row new_row
	for each row insert into t1 values(1, 'update_new_row');

create trigger test3
	after update on t1 referencing new as new_row
	for each row insert into t1 values(2, 'update_new_row');

create trigger test4
	after update on t1 referencing new new_row
	for each row insert into t1 values(3, 'update_new_row');


update t1 set name = 'mo' where id = 10;

select * from t1;

delete from t1 where id >1;

drop trigger test1;
drop trigger test2;
drop trigger test3;
drop trigger test4;

--test when the trigger event is INSERT
insert into t1 values(10, 'monetdb');
insert into t1 values(20, 'monet');

create trigger test1
	after insert on t1 referencing new row as new_row
	for each row insert into t1 values(0, 'insert_new_row');

create trigger test2
	after insert on t1 referencing new row new_row
	for each row insert into t1 values(1, 'insert_new_row');

create trigger test3
	after insert on t1 referencing new as new_row
	for each row insert into t1 values(2, 'insert_new_row');

create trigger test4
	after insert on t1 referencing new new_row
	for each row insert into t1 values(3, 'insert_new_row');


select * from t1;

delete from t1 where id >1;

drop trigger test1;
drop trigger test2;
drop trigger test3;
drop trigger test4;

--test error messages
--new row and new table are not allowed if the Trigger event is DELETE

insert into t1 values(10, 'monetdb');
insert into t1 values(20, 'monet');

create trigger test1
	after delete on t1 referencing new row as new_row
	for each row insert into t1 values(0, 'delete_new_row');

create trigger test2
	after delete on t1 referencing new row new_row
	for each row insert into t1 values(1, 'delete_new_row');

create trigger test3
	after delete on t1 referencing new as new_row
	for each row insert into t1 values(2, 'delete_new_row');

create trigger test4
	after delete on t1 referencing new new_row
	for each row insert into t1 values(3, 'delete_new_row');


select * from t1;

delete from t1 where id >1;

drop trigger test1;
drop trigger test2;
drop trigger test3;
drop trigger test4;

--Cleanup
drop table t1;
