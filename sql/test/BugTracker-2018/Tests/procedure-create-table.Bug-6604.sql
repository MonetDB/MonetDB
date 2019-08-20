create procedure test() begin create table x (i int); insert into x values (1), (2); end;
create procedure test() begin create table x as select name from sys.functions with data; end;

call test();
select i from x limit 1;
\d x
\df test
drop procedure test;

create procedure test() begin create table x as select name from sys.functions with data; end;

call test();
select name from x limit 1;
\d x
\df test
drop procedure test;

