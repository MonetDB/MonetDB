--this test only tests the sintax
--the semantic should also be tested after the syntax test

create table t1 (id int, name varchar(1024));
create table t2 (id int);

insert into t1 values(10, 'monetdb');
insert into t1 values(20, 'monet');


create trigger test1
	after update on t1
	delete from t1 where name = 'mo' and id = 10;

create trigger test2
	before update on t1
	select id into t2 from t1 where id = 10;

create trigger test3
	after update on t1
	insert into t1 values(10, 'update_from_test3');

create trigger test4
	after update on t1
	insert into t1 values(20, 'update_from_test4');

ALTER TABLE t1 add CONSTRAINT t1_constraint PRIMARY KEY(id);

update t1 set name = 'mo' where id = 10;

select * from t1;

select * from t2;

delete from t1 where id >1;

drop trigger test1;
drop trigger test2;
drop trigger test3;
drop trigger test4;
alter table t1 drop constraint t1_constraint;


--Cleanup
drop table t1;
drop table t2;
