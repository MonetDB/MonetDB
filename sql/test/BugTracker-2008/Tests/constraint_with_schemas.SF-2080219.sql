create schema s1;
create schema s2;

create table s1.A(a varchar(10) NOT NULL UNIQUE);
create table s2.A(a varchar(10) NOT NULL);

set schema s2;
insert into A values('abc');
set schema s1;
insert into A values('abc');

select * from s1.A;
select * from s2.A;

drop table s1.A;
drop table s2.A;

create table s1.A(a varchar(10) NOT NULL UNIQUE);
create table s2.A(a varchar(10) NOT NULL);

set schema s1;
insert into A values('abc');
set schema s2;
insert into A values('abc');

drop table s1.A;
drop table s2.A;

set schema sys;
drop schema s2;
drop schema s1;
