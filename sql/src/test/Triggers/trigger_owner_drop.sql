--test the owner restriction for triggers

create trigger test2 on t2
	insert into t1 values(12);

drop trigger test1;
drop trigger test2;
drop table t1;
drop table t2;
drop user user_test;
