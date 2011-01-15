--test if it is allowed to execute schema operations with triggers

create table t1 (id int, name varchar(1024));

insert into t1 values(1, 'monetdb');

create trigger test1
	after update on t1
	create table t2 (id int);

update t1 set name = 'mo' where id = 1;

select name from tables where name = 't2';

drop trigger test1;

create table t2(id int);

create trigger test1
	after update on t1
	drop table t2;

update t1 set name = 'mo' where id = 1;

select name from tables where name = 't2';

drop trigger test1;

create trigger test1
	after update on t1
	alter table t2 add column (name varchar(1024));

create trigger test2
	after update on t1
	create trigger test2_1
		after update on t1
		alter table t2 add column (name varchar(1024));

create trigger test3
	after update on t1
	create view v1 as select * from t1;

create trigger test4
	after update on t1
	create function f1 (id int)
	returns boolean
	BEGIN
		if (id >0)
			then return true;
			else return false;
		end if;
	END;

create trigger test5
	after update on t1
	create index id_index on t2(id);

select name from tables where name = 'v1';

select name from functions where name = 'f1';

select name from idxs where name = 'id_index';

drop trigger test1;
drop trigger test2;
drop trigger test3;
drop trigger test4;
drop trigger test5;

drop table t2;


--Cleanup
drop table t1;

