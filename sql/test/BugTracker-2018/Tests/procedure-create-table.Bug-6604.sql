create procedure test() begin create table x (i int); insert into x values (1), (2); end;
call test();
select i from x limit 1;
drop table x;
drop procedure test;

create procedure test() begin create table x as select * from sys.functions with data; end;
call test();
select query from x limit 1;
drop table x;
drop procedure test;
