--test the owner restriction for triggers

create trigger test_6_1 after insert on t_6_1
	insert into t_6_1 values(12);

create table t_6_2(age int);
