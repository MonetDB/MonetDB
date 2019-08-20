select 1;
CREATE TABLE foo (id INTEGER, name VARCHAR(20));
INSERT INTO foo VALUES (1, 'Tim');
INSERT INTO foo VALUES (2, 'Jochem');
drop table foo;
     
select 1;
CREATE TABLE ff(id INTEGER, name VARCHAR(20));
INSERT INTO ff VALUES (1, 'Tim');
INSERT INTO ff VALUES (2, 'Jochem');
select * from ff;
drop table ff;

select (4-1)*5;
--select current_date;

select name, query, "type", system, commit_action from _tables
	where name like 'foo' or name like 'ff';

create table s4(i time);
drop table s4;

create table r(i int);
insert into r values(1);
insert into r values(2);
select * from r;

delete from r where i>1;
select * from r;

select * from r;

select name, query, "type", system, commit_action from _tables
	where name in ('s4', 'r', 'foo', 'ff');

drop table r;
create table r(i int);
insert into r values(1);
insert into r values(2);
select * from r;

drop table r;
create table r(i int);
insert into r values(1);
insert into r values(2);
delete from r where i>1;
select * from r;

select * from r;

drop table r;
create table r(i int);
insert into r values(1);

drop table r;

-- next query shouldn't work
select name, count(*) from _tables;
-- this should
select name, 1, 2, 3  from _tables
	where name in ('s4', 'r', 'foo', 'ff');
