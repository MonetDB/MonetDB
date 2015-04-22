create table t1 (i int);
create table t2 (i int);
create merge table m (i int);
alter table m add table t1;
alter table m add table t2;
insert into m values (1);

drop table m;
drop table t2;
drop table t1;

