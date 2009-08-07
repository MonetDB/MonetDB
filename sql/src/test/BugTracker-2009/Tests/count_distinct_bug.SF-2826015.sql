create table testdistinct (testcol varchar(10));
insert into testdistinct values ('test'),('test'), ('test'), ('other'), ('other'), ('other');
select * from testdistinct ;
select distinct(testcol) from testdistinct ;
select count(distinct(testcol)) from testdistinct ;
select count(distinct testcol ) from testdistinct ;
drop table testdistinct;
