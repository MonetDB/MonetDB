start transaction;
-- simple 'standard' case (dropping in the right order)
create table a (id int primary key);
create table b (id int references a(id));
drop table b;
drop table a;
rollback;

start transaction;
-- check that the dependency of b on a is checked
create table a (id int primary key);
create table b (id int references a(id));
drop table a;
drop table b;
rollback;

start transaction;
-- self references: don't bail out on references that are from the same table
create table tmp (i integer unique not null, b integer references tmp(i));
drop table tmp;
rollback;
