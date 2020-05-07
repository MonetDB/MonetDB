create schema foo;

select current_schema; --sys

start transaction;
select current_schema; --sys
set schema foo;
select current_schema; --foo
rollback;

select current_schema; --sys

drop schema foo;
