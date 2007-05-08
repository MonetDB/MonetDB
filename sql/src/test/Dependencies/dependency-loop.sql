-- [ 1714809 ] Infinite loop....
create table t1Infi(id int, name varchar(1024), age int);
create table t2Infi(id int, age int);

create PROCEDURE p1(id int, age int)
BEGIN
insert into t2Infi values(id, age);
END;

create PROCEDURE p1()
BEGIN
declare id int, age int;
set id = 1;
set age = 23;
call p1(id, age);
END;

create trigger test_0 after insert on t2Infi
BEGIN ATOMIC
insert into t1Infi values(1, 'monetdb', 24);
call p1();
END;

drop trigger test_0;
drop procedure p1;
drop table t1Infi;
drop table t2Infi;

