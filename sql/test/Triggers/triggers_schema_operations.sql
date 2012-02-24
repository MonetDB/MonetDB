--test if it is allowed to execute schema operations with triggers

create table t_7_1 (id int, name varchar(1024));

insert into t_7_1 values(1, 'monetdb');

create trigger test_7_1
	after update on t_7_1
	create table t_7_2 (id int);

update t_7_1 set name = 'mo' where id = 1;

select name from tables where name = 't_7_2';

drop trigger test_7_1;

create table t_7_2(id int);

create trigger test_7_1
	after update on t_7_1
	drop table t_7_2;

update t_7_1 set name = 'mo' where id = 1;

select name from tables where name = 't_7_2';

drop trigger test_7_1;

create trigger test_7_1
	after update on t_7_1
	alter table t_7_2 add column (name varchar(1024));

create trigger test_7_2
	after update on t_7_1
	create trigger test_7_2_1
		after update on t_7_1
		alter table t_7_2 add column (name varchar(1024));

create trigger test_7_3
	after update on t_7_1
	create view v1 as select * from t_7_1;

create trigger test_7_4
	after update on t_7_1
	create function f1 (id int)
	returns boolean
	BEGIN
		if (id >0)
			then return true;
			else return false;
		end if;
	END;

create trigger test_7_5
	after update on t_7_1
	create index id_index on t_7_2(id);

select name from tables where name = 'v1';

select name from functions where name = 'f1';

select name from idxs where name = 'id_index';

drop trigger test_7_1;
drop trigger test_7_2;
drop trigger test_7_3;
drop trigger test_7_4;
drop trigger test_7_5;

drop table t_7_2;


--Cleanup
drop table t_7_1;

