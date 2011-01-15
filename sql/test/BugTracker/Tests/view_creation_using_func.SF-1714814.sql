create table t1714814a(id int, name varchar(1024), age int);

create function f1()
returns int
BEGIN
	return 1;
END;

create view v1 as select * from t1714814a where id = f(1);

drop view v1;
drop table t1714814a;
drop function f1(); 
