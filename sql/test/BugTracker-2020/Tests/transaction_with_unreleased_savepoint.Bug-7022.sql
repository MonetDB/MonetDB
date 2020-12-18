-- this works
start transaction;
create table savepointtest (id int, primary key(id));
savepoint name1;
insert into savepointtest values(24);
release savepoint name1;
commit;
select name, schema_id, query, type, system, commit_action, access, temporary from tables where name = 'savepointtest';
select * from savepointtest;

create table savepointtest (id int, primary key(id));
insert into savepointtest values(42);
select name, schema_id, query, type, system, commit_action, access, temporary from tables where name = 'savepointtest';
select * from savepointtest;
drop table savepointtest;
select name, schema_id, query, type, system, commit_action, access, temporary from tables where name = 'savepointtest';

-- this shows errors
start transaction;
create table savepointtest (id int, primary key(id));
savepoint name1;
insert into savepointtest values(24);
commit;
select name, schema_id, query, type, system, commit_action, access, temporary from tables where name = 'savepointtest';
select * from savepointtest;

create table savepointtest (id int, primary key(id));
insert into savepointtest values(42);
select name, schema_id, query, type, system, commit_action, access, temporary from tables where name = 'savepointtest';
-- the 2nd 'savepointtest` is used by SELECT and DROP
select * from savepointtest;
drop table savepointtest;
select name, schema_id, query, type, system, commit_action, access, temporary from tables where name = 'savepointtest';
