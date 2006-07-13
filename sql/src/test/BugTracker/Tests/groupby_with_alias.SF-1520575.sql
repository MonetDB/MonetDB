set DEBUG=4096;
START TRANSACTION;

create table test(a int);
select a as b from test group by b; 

create table a(f1 varchar(20), f2 int);
select coalesce(f1,'EMPTY') as bug_alias, sum(f2) from a group by bug_alias;

ROLLBACK;
