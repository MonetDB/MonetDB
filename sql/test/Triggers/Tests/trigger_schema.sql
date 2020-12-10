-- Verify the trigger's schema is the one set every time the trigger is fired
START TRANSACTION;
create schema foo;
set schema foo;
create table t1 (id serial, ref bigint);
create table t2 (id serial, ref bigint);

create function f(ref bigint) returns bigint begin return 10*ref; end;

create schema bar;
set schema bar;
create function f(ref bigint) returns bigint begin return 100*ref; end;
create table t1 (id serial, ref bigint);
create table t2 (id serial, ref bigint);

create trigger extra_insert
    AFTER INSERT ON t1 referencing new row as new_row
    FOR EACH statement insert into t2(ref) values (f(new_row.ref));

insert into t1(ref) values (10);
set schema foo;
insert into bar.t1(ref) values (10);

select * from bar.t1;
select * from bar.t2;
select * from foo.t1;
select * from foo.t2;
ROLLBACK;

START TRANSACTION;
create schema foo;
set schema foo;
create table t1 (id serial, ref bigint);
create table t2 (id serial, ref bigint);
create schema bar;
set schema bar;
create trigger foo.extra_insert
    AFTER INSERT ON t1 referencing new row as new_row
    FOR EACH statement insert into t2(ref) values (new_row.ref); --error, a trigger will be placed on its table's schema, specify the schema on the table reference, ie ON clause instead
ROLLBACK;
