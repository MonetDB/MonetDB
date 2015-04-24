create table t1 (i int);
create table t2 (i int);
create merge table m (i int);
alter table m add table t1;
alter table m add table t2;
alter table m drop table t1;
alter table m drop table t1;
DROP table t1;
alter table m drop table t1;

alter table m drop table t2;
DROP table t2;
DROP table m;
