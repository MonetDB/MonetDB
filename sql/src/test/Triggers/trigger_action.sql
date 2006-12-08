--this test only tests the sintax
--the semantic should also be tested after the syntax test

create table t1 (id int, name varchar(1024));

--test FOR EACH STATMENT (default one)
insert into t1 values(10, 'monetdb');
insert into t1 values(20, 'monet');


create trigger test1
	after update on t1 referencing old row as old_row
	for each statement insert into t1 values(0, 'update_old_row_statement');

create trigger test2
	after update on t1 referencing new row new_row
	for each statement insert into t1 values(1, 'update_new_row_statement');

create trigger test3
	after update on t1
	for each statement insert into t1 values(2, 'update_statement');

create trigger test4
	after update on t1 referencing new row as new_row
	for each row insert into t1 values(2, 'update_row');

create trigger test5
	after update on t1
	for each statement 
	when id >0 insert into t1 values(2, 'update_when_statement_true');

--tes WHEN clause

create trigger test6
	after update on t1 referencing new row as new_row
	for each row 
	when new_row.id >0 insert into t1 values(2, 'update_when_row_true');

create trigger test7
	after update on t1
	for each statement 
	when id >1000 insert into t1 values(2, 'update_when_statement_false');

create trigger test8
	after update on t1 referencing new row as new_row
	for each row 
	when new_row.id >1000 insert into t1 values(2, 'update_when_row_false');

update t1 set name = 'mo' where id = 10;

select * from t1;

delete from t1 where id >1;

drop trigger test1;
drop trigger test2;
drop trigger test3;
drop trigger test4;
drop trigger test5;
drop trigger test6;
drop trigger test7;
drop trigger test8;

--Test ACTION BODY
insert into t1 values(10, 'monetdb');
insert into t1 values(20, 'monet');


create trigger test1
	after update on t1
	BEGIN ATOMIC
		insert into t1 values(1,'first_insertion');
		insert into t1 values(2,'second_insertion');
	END;

create trigger test2
	after update on t1
	insert into t1 values(3,'third_insertion');
	insert into t1 values(4,'fourth_insertion');

create trigger test3
	after update on t1
	BEGIN ATOMIC
		insert into t1 values(5,'fifth_insertion');
	END;

create trigger test4
	after update on t1
	BEGIN ATOMIC
		insert into t1 values(1,'first_insertion');
		insert into t1 values(2,'second_insertion');
	END;

select * from t1;

delete from t1 where id >1;

drop trigger test1;
drop trigger test2;
drop trigger test3;
drop trigger test4;

--Cleanup
drop table t1;
