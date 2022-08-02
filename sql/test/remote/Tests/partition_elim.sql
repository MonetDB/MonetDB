--Create merge table and partition tables
create merge table test (x double, y double);
create table test1 (x double, y double);
create table test2 (x double, y double);
create table test3 (x double, y double);
create table test4 (x double, y double);

--Populate tables
insert into test1 values (1.0, 1.0);
insert into test1 values (2.0, 2.0);
insert into test2 values (3.0, -1.0);
insert into test2 values (4.0, -2.0);
insert into test3 values (3.0, 1.0);
insert into test3 values (6.0, 2.0);
insert into test4 values (7.0, 1.0);
insert into test4 values (10.0, 2.0);

--Set tables to read only
alter table test1 set read only;
alter table test2 set read only;
alter table test3 set read only;
alter table test4 set read only;

--Add partitions to merge table
alter table test add table test1;
alter table test add table test2;
alter table test add table test3;
alter table test add table test4;

--Build imprints
select x from test1 where x between 0 and -1;
select x from test2 where x between 0 and -1;
select x from test3 where x between 0 and -1;
select x from test4 where x between 0 and -1;

--Build imprints
select y from test1 where y between 0 and -1;
select y from test2 where y between 0 and -1;
select y from test3 where y between 0 and -1;
select y from test4 where y between 0 and -1;

--Analyze tables
analyze sys.test1 minmax;
analyze sys.test2 minmax;
analyze sys.test3 minmax;
analyze sys.test4 minmax;

--Test query: Only partition 2 and 3 should be selected
explain select x,y from test where x between 4.0 and 6.0;

--Test query: Only partition 2 and 3 should be selected
explain select x,y from test where x between (7-3) and (7-1);

--Test query: Only partition 3 should be selected
explain select x,y from test where x between 4.0 and 6.0 and y between 0.0 and 2.0;

--Test query: Only partition 3 should be selected
explain select x,y from test where x between 4.0 and 6.0 and y between (1.0-1.0) and (4.0-2.0);

--Test query: Only partition 3 should be selected
explain select x,y from test where x between (7-3) and (7-1) and y between (1.0-1.0) and (4.0-2.0);

--Drop test tables
drop table test;
drop table test1;
drop table test2;
drop table test3;
drop table test4;
