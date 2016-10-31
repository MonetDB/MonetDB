--this test only tests the sintax
--the semantic should also be tested after the syntax test

create table t_4_1 (id int, name varchar(1024));

--test FOR EACH STATMENT (default one)
insert into t_4_1 values(10, 'monetdb');
insert into t_4_1 values(20, 'monet');


create trigger test_4_1
	after update on t_4_1 referencing old row as old_row
	for each statement insert into t_4_1 values(0, 'update_old_row_statement');

create trigger test_4_2
	after update on t_4_1 referencing new row new_row
	for each statement insert into t_4_1 values(1, 'update_new_row_statement');

create trigger test_4_3
	after update on t_4_1
	for each statement insert into t_4_1 values(2, 'update_statement');

create trigger test_4_4
	after update on t_4_1 referencing new row as new_row
	for each row insert into t_4_1 values(3, 'update_row');

create trigger test_4_5
	after update on t_4_1
	for each statement 
	when (id>0) insert into t_4_1 values(4, 'update_when_statement_true');

--test WHEN clause

create trigger test_4_6
	after update on t_4_1 referencing new row as new_row
	for each row 
	when (new_row.id>0) insert into t_4_1 values(5, 'update_when_row_true');

create trigger test_4_7
	after update on t_4_1
	for each statement 
	when (id >1000) insert into t_4_1 values(6, 'update_when_statement_false');

create trigger test_4_8
	after update on t_4_1 referencing new row as new_row
	for each row 
	when (new_row.id>1000) insert into t_4_1 values(7, 'update_when_row_false');

update t_4_1 set name = 'mo' where id = 10;

select * from t_4_1;

delete from t_4_1 where id >-1;

drop trigger test_4_1;
drop trigger test_4_2;
drop trigger test_4_3;
drop trigger test_4_4;
drop trigger test_4_5;
drop trigger test_4_6;
drop trigger test_4_7;
drop trigger test_4_8;

--Test ACTION BODY
insert into t_4_1 values(10, 'monetdb');
insert into t_4_1 values(20, 'monet');


create trigger test_4_1
	after update on t_4_1
	BEGIN ATOMIC
		insert into t_4_1 values(1,'first_insertion');
		insert into t_4_1 values(2,'second_insertion');
	END;

create trigger test_4_2
	after update on t_4_1
	BEGIN ATOMIC
		insert into t_4_1 values(3,'third_insertion');
	END;

create trigger test_4_3
	after update on t_4_1
	BEGIN ATOMIC
		insert into t_4_1 values(4,'fourth_insertion');
		insert into t_4_1 values(5,'fifth_insertion');
	END;


update t_4_1 set name = 'mo' where id = 10;

select * from t_4_1;

drop trigger test_4_1;
drop trigger test_4_2;
drop trigger test_4_3;

--Cleanup
drop table t_4_1;
