--this test only tests the syntax
--the semantic should also be tested after the syntax test
create table t_5_1 (id int, name varchar(1024));

-- alter early as its, executed as update (one of the key columns).
ALTER TABLE t_5_1 add CONSTRAINT t_5_1_constraint PRIMARY KEY(id);

insert into t_5_1 values(10, 'monetdb');
insert into t_5_1 values(20, 'monet');

create trigger test_5_1
	after update on t_5_1
	delete from t_5_1 where name = 'monetdb' and id = 10;

-- first we need to know what to update (no id=11 jet)
-- ie this trigger will no fire.
create trigger test_5_2
	before update on t_5_1
	insert into t_5_1 values(11,'amsterdam');

create trigger test_5_3
	after update on t_5_1
	insert into t_5_1 values(10, 'update_from_test_5_3');

create trigger test_5_4
	after update on t_5_1
	insert into t_5_1 values(30, 'update_from_test_5_4');

update t_5_1 set name = 'mo' where id = 11;

select * from t_5_1;

delete from t_5_1 where id >1;

drop trigger test_5_1;
drop trigger test_5_2;
drop trigger test_5_3;
drop trigger test_5_4;
alter table t_5_1 drop constraint t_5_1_constraint;


--Cleanup
drop table t_5_1;
