create schema s1;
create schema s2;
set schema s1;
create table s3 (x int);
create view s4 as select * from s3;
select * from s1.s4;
set schema s2;
select * from s1.s4;

set schema sys;
drop schema s1;
drop schema s2;

create schema s1;
create schema s2;
create table s1.s3 (x int);
create table s2.s3 (x int);
set schema s1;
create view s4 as select * from s3;
plan select * from s1.s4;
set schema s2;
plan select * from s1.s4;

set schema sys;
drop schema s1;
drop schema s2;
