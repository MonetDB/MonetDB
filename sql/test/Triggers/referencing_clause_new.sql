--this test only tests the sintax
--the semantic should also be tested after the syntax test

create table t_2_1 (id int, name varchar(1024));
create table t_2_2 (id int, name varchar(1024));

--test when trigger event is UPDATE
insert into t_2_1 values(10, 'monetdb');
insert into t_2_1 values(20, 'monet');


create trigger test_2_1
	after update on t_2_1 referencing new row as new_row
	for each row insert into t_2_2 values(0, 'update_new_row');

create trigger test_2_2
	after update on t_2_1 referencing new row new_row
	for each row insert into t_2_2 values(1, 'update_new_row');

create trigger test_2_3
	after update on t_2_1 referencing new as new_row
	for each row insert into t_2_2 values(2, 'update_new_row');

create trigger test_2_4
	after update on t_2_1 referencing new new_row
	for each row insert into t_2_2 values(3, 'update_new_row');


update t_2_1 set name = 'mo' where id = 10;

select * from t_2_1;
select * from t_2_2;

delete from t_2_1 where id > -1;
delete from t_2_2 where id > -1;

drop trigger test_2_1;
drop trigger test_2_2;
drop trigger test_2_3;
drop trigger test_2_4;

--test when trigger event is DELETE
insert into t_2_1 values(10, 'monetdb');
insert into t_2_1 values(20, 'monet');

create trigger test_2_1
	after delete on t_2_1 referencing new row as new_row
	for each row insert into t_2_2 values(0, 'delete_new_row');

create trigger test_2_2
	after delete on t_2_1 referencing new row new_row
	for each row insert into t_2_2 values(1, 'delete_new_row');

create trigger test_2_3
	after delete on t_2_1 referencing new as new_row
	for each row insert into t_2_2 values(2, 'delete_new_row');

create trigger test_2_4
	after delete on t_2_1 referencing new new_row
	for each row insert into t_2_2 values(3, 'delete_new_row');


delete from t_2_1 where id >-1;

select * from t_2_1;
select * from t_2_2;

drop trigger test_2_1;
drop trigger test_2_2;
drop trigger test_2_3;
drop trigger test_2_4;

delete from t_2_2 where id >-1;

--test error messages
--new row and new table are not allowed if the Trigger event is INSERT

insert into t_2_1 values(10, 'monetdb');

create trigger test_2_1
	after insert on t_2_1 referencing new row as new_row
	for each row insert into t_2_2 values(0, 'insert_new_row');

create trigger test_2_2
	after insert on t_2_1 referencing new row new_row
	for each row insert into t_2_2 values(1, 'insert_new_row');

create trigger test_2_3
	after insert on t_2_1 referencing new as new_row
	for each row insert into t_2_2 values(2, 'insert_new_row');

create trigger test_2_4
	after insert on t_2_1 referencing new new_row
	for each row insert into t_2_2 values(3, 'insert_new_row');


insert into t_2_1 values(20, 'monet');

select * from t_2_1;
select * from t_2_2;

delete from t_2_1 where id >-1;
delete from t_2_2 where id >-1;

drop trigger test_2_1;
drop trigger test_2_2;
drop trigger test_2_3;
drop trigger test_2_4;

--test with new row and new table and mixed 

insert into t_2_1 values(10, 'monetdb');
insert into t_2_1 values(20, 'monet');

create trigger test_2_1
	after update on t_2_1 referencing new row as new_row new table as new_table
	for each row insert into t_2_2 values(0, 'insert_new_row_table');

create trigger test_2_2
	after update on t_2_1 referencing new row new_row new row as new_row
	for each row insert into t_2_2 values(1, 'insert_new_new_row');

create trigger test_2_3
	after update on t_2_1 referencing new table as new_table new table as new_table
	for each row insert into t_2_2 values(2, 'insert_new__new_table');

create trigger test_2_4
	after update on t_2_1 referencing new row as new_row new table as new_table
	for each row insert into t_2_2 values(3, 'insert_new_row_new_table');

create trigger test_2_5
	after update on t_2_1 referencing new table as new_table new row as new_row
	for each row insert into t_2_2 values(4, 'insert_new_table_new_row');


update t_2_1 set name = 'mo' where id = 10;

select * from t_2_1;
select * from t_2_2;

delete from t_2_1 where id >-1;
delete from t_2_2 where id >-1;


drop trigger test_2_1;
drop trigger test_2_2;
drop trigger test_2_3;
drop trigger test_2_4;
drop trigger test_2_5;

--test stanger combinations

insert into t_2_1 values(10, 'monetdb');

create trigger test_2_1
	after update on t_2_1 referencing new row as new_row new table as new_table
	for each row insert into t_2_2 values(0, 'update_new_row_new_table');

create trigger test_2_2
	after insert on t_2_1 referencing new row new_row new row as new_row
	for each row insert into t_2_2 values(1, 'insert_new_new_row');

create trigger test_2_3
	after delete on t_2_1 referencing new row new_row new row as new_row
	for each row insert into t_2_2 values(2, 'delete_new_new_row');

create trigger test_2_4
	after delete on t_2_1 referencing new row as new_row new table as new_table
	for each row insert into t_2_2 values(3, 'delete_new_row_new_table');

create trigger test_2_5
	after insert on t_2_1 referencing new table as new_table new row as new_row
	for each row insert into t_2_2 values(4, 'insert_new_table_new_row');

insert into t_2_1 values(20, 'monet');
select * from t_2_1;
select * from t_2_2;

update t_2_1 set name = 'mo' where id = 10;
select * from t_2_1;
select * from t_2_2;

delete from t_2_1 where id >5;
select * from t_2_1;
select * from t_2_2;


drop trigger test_2_1;
drop trigger test_2_2;
drop trigger test_2_3;
drop trigger test_2_4;
drop trigger test_2_5;

delete from t_2_1 where id >-1;
delete from t_2_2 where id >-1;

--Cleanup
drop table t_2_1;
drop table t_2_2;
