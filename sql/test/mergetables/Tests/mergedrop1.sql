drop table t;
drop table t1;
drop table t2;
drop table t3;

create merge table t(x double);
create table t1(x double);
create table t2(x double);
create table t3(x double);

insert into t1 values(1);
insert into t2 values(1);
insert into t3 values(1);

alter table t1 set read only;
alter table t2 set read only;
alter table t3 set read only;

alter table t add table t1;
alter table t add table t2;
alter table t add table t3;

alter table t drop table t1;
alter table t add table t1;
