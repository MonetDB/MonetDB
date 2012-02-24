--test the owner restriction for triggers

create trigger test_6_2 after insert on t_6_2
	insert into t_6_1 values(12);

drop trigger test_6_1;
drop trigger test_6_2;
drop table t_6_1;
drop table t_6_2;
drop user user_test;
