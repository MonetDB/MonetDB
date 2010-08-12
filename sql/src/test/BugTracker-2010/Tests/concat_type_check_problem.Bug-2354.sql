create table test2(a varchar(256), b int, c varchar(16));
insert into test2 values ('testaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 'test');
select a||' '||b||' '||c from test2;
select cast(a||' '||b as varchar(256))||' '||c from test2;
drop table test2;

create table testmore2(test1 varchar(1), test2 int, test3 varchar(3));
insert into testmore2 values ('1', 23, '456');
select length(test1||' '||test2||' '||test3) from testmore2 ;
drop table testmore2;
