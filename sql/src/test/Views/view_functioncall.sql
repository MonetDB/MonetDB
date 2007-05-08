--[ 1714814 ] View creation using a function....
create table t1(id int, name varchar(1024), age int);
create table t2(id int, age int);

create function f1()
returns int
BEGIN
return 1;
END;

create view v1 as select * from t1 where id = f(1);
