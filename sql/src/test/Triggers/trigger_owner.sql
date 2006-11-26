--test the owner restriction for triggers

create trigger test1 on t1
	insert into t1 values(12);

create table t2(age int);
