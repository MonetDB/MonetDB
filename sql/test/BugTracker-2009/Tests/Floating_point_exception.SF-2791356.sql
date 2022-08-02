create table test2791356 (bla1 int, bla2 int);
insert into test2791356 values (1,1);
select case when (bla1 - bla2) > 0 then 1/(bla1 - bla2) else 0 end from test2791356;
drop table test2791356;
