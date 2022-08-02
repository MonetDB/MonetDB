create merge table test1 (a int);
create merge table test2 (a int);
create table test3 (like test1);
create merge table test4 (a int);
create table test5 (like test4);

insert into test3 values (1);

alter table test1 add table test2;
alter table test2 add table test3;
alter table test2 add table test1; --error

select a from test1;
select a from test2;
select a from test3;

alter table test2 add table test4;
alter table test4 add table test1; --error
alter table test4 add table test5;

select a from test1;
select a from test2;

alter table test1 drop table test2;
alter table test2 drop table test3;
alter table test2 drop table test4;
alter table test4 drop table test1; --error
alter table test4 drop table test5;

drop table test1;
drop table test2;
drop table test3;
drop table test4;
drop table test5;
