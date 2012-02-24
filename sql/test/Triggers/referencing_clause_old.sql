--this test only tests the sintax
--the semantic should also be tested after the syntax test

create table t_3_1 (id int, name varchar(1024));
create table t_3_2 (id int, name varchar(1024));

--test when trigger event is UPDATE
insert into t_3_1 values(10, 'monetdb');
insert into t_3_1 values(20, 'monet');


create trigger test_3_1
	after update on t_3_1 referencing old row as old_row
	for each row insert into t_3_2 values(0, 'update_old_row');

create trigger test_3_2
	after update on t_3_1 referencing old row old_row
	for each row insert into t_3_2 values(1, 'update_old_row');

create trigger test_3_3
	after update on t_3_1 referencing old as old_row
	for each row insert into t_3_2 values(2, 'update_old_row');

create trigger test_3_4
	after update on t_3_1 referencing old old_row
	for each row insert into t_3_2 values(3, 'update_old_row');


update t_3_1 set name = 'mo' where id = 10;

select * from t_3_1;
select * from t_3_2;

delete from t_3_1 where id > -1;
delete from t_3_2 where id > -1;

drop trigger test_3_1;
drop trigger test_3_2;
drop trigger test_3_3;
drop trigger test_3_4;

--test when trigger event is DELETE
insert into t_3_1 values(10, 'monetdb');
insert into t_3_1 values(20, 'monet');

create trigger test_3_1
	after delete on t_3_1 referencing old row as old_row
	for each row insert into t_3_2 values(0, 'delete_old_row');

create trigger test_3_2
	after delete on t_3_1 referencing old row old_row
	for each row insert into t_3_2 values(1, 'delete_old_row');

create trigger test_3_3
	after delete on t_3_1 referencing old as old_row
	for each row insert into t_3_2 values(2, 'delete_old_row');

create trigger test_3_4
	after delete on t_3_1 referencing old old_row
	for each row insert into t_3_2 values(3, 'delete_old_row');


delete from t_3_1 where id >-1;

select * from t_3_1;
select * from t_3_2;

drop trigger test_3_1;
drop trigger test_3_2;
drop trigger test_3_3;
drop trigger test_3_4;

delete from t_3_2 where id >-1;

--test error messages
--old row and old table are not allowed if the Trigger event is INSERT

insert into t_3_1 values(10, 'monetdb');

create trigger test_3_1
	after insert on t_3_1 referencing old row as old_row
	for each row insert into t_3_2 values(0, 'insert_old_row');

create trigger test_3_2
	after insert on t_3_1 referencing old row old_row
	for each row insert into t_3_2 values(1, 'insert_old_row');

create trigger test_3_3
	after insert on t_3_1 referencing old as old_row
	for each row insert into t_3_2 values(2, 'insert_old_row');

create trigger test_3_4
	after insert on t_3_1 referencing old old_row
	for each row insert into t_3_2 values(3, 'insert_old_row');


insert into t_3_1 values(20, 'monet');

select * from t_3_1;
select * from t_3_2;

delete from t_3_1 where id >-1;
delete from t_3_2 where id >-1;

drop trigger test_3_1;
drop trigger test_3_2;
drop trigger test_3_3;
drop trigger test_3_4;

--test with old row and old table and mixed 

insert into t_3_1 values(10, 'monetdb');
insert into t_3_1 values(20, 'monet');

create trigger test_3_1
	after update on t_3_1 referencing old row as old_row old table as old_table
	for each row insert into t_3_2 values(0, 'insert_old_row_table');

create trigger test_3_2
	after update on t_3_1 referencing old row old_row new row as new_row
	for each row insert into t_3_2 values(1, 'insert_old_new_row');

create trigger test_3_3
	after update on t_3_1 referencing old table as old_table new table as new_table
	for each row insert into t_3_2 values(2, 'insert_old__new_table');

create trigger test_3_4
	after update on t_3_1 referencing old row as old_row new table as new_table
	for each row insert into t_3_2 values(3, 'insert_old_row_new_table');

create trigger test_3_5
	after update on t_3_1 referencing old table as old_table new row as new_row
	for each row insert into t_3_2 values(4, 'insert_old_table_new_row');


update t_3_1 set name = 'mo' where id = 10;

select * from t_3_1;
select * from t_3_2;

delete from t_3_1 where id >-1;
delete from t_3_2 where id >-1;


drop trigger test_3_1;
drop trigger test_3_2;
drop trigger test_3_3;
drop trigger test_3_4;
drop trigger test_3_5;

--test stanger combinations

insert into t_3_1 values(10, 'monetdb');

create trigger test_3_1
	after update on t_3_1 referencing old row as old_row new table as new_table
	for each row insert into t_3_2 values(0, 'update_old_row_new_table');

create trigger test_3_2
	after insert on t_3_1 referencing old row old_row new row as new_row
	for each row insert into t_3_2 values(1, 'insert_old_new_row');

create trigger test_3_3
	after delete on t_3_1 referencing old row old_row new row as new_row
	for each row insert into t_3_2 values(2, 'delete_old_new_row');

create trigger test_3_4
	after delete on t_3_1 referencing old row as old_row new table as new_table
	for each row insert into t_3_2 values(3, 'delete_old_row_new_table');

create trigger test_3_5
	after insert on t_3_1 referencing old table as old_table new row as new_row
	for each row insert into t_3_2 values(4, 'insert_old_table_new_row');

insert into t_3_1 values(20, 'monet');
select * from t_3_1;
select * from t_3_2;

update t_3_1 set name = 'mo' where id = 10;
select * from t_3_1;
select * from t_3_2;

delete from t_3_1 where id >5;
select * from t_3_1;
select * from t_3_2;


drop trigger test_3_1;
drop trigger test_3_2;
drop trigger test_3_3;
drop trigger test_3_4;
drop trigger test_3_5;

delete from t_3_1 where id >-1;
delete from t_3_2 where id >-1;

--Cleanup
drop table t_3_1;
drop table t_3_2;
