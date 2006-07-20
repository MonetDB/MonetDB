START TRANSACTION;

create table test_grp(a int);
select a as b from test_grp group by b; 

create table a(f1 varchar(20), f2 int);
select coalesce(f1,'EMPTY') as bug_alias, sum(f2) from a group by bug_alias;

ROLLBACK;
