create table t1(t int);
insert into t1 values(1);

create table t2(t int);
insert into t2 values(2);

create merge table  tt(t int);
alter table tt add table t1;
alter table tt add table t2;

select * from tt;

-- update tt set t = 3 where t = 1;
-- update tt set t = 4;

delete from tt where t = 1;
delete from tt where t = 2;
delete from tt;

drop table tt cascade;
drop table t1 cascade;
drop table t2 cascade;

