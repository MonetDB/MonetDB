
select 1;
CREATE TABLE foo (id INTEGER, name VARCHAR(20));
INSERT INTO foo VALUES (1, 'Tim');
INSERT INTO foo VALUES (2, 'Jochem');
     
select 1;
CREATE TABLE ff(id INTEGER, name VARCHAR(20));
INSERT INTO ff VALUES (1, 'Tim');
INSERT INTO ff VALUES (2, 'Jochem');
select * from ff;

select (4-1)*5;
--select current_date;

select name, query, istable, system, clear from ptables;

create table s4(i time);

commit work;
create table r(i int);
insert into r values(1);
insert into r values(2);
select * from r;

delete from r where i>1;
select * from r;

rollback;
select * from r;
rollback;

select name, query, istable, system, clear from ptables;
create table r(i int);
insert into r values(1);
insert into r values(2);
rollback;
select * from r;

commit work;
create table r(i int);
insert into r values(1);
insert into r values(2);
delete from r where i>1;
select * from r;

rollback;
select * from r;
rollback;

create table r(i int);
insert into r values(1);
commit work;

drop table r;

-- next query shouldn't work
select name, count(*) from ptables;
rollback;
-- this should
select name, 1, 2, 3  from ptables;



