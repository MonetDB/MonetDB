set schema sys;
set schema json;
set role monetdb;

declare a int;
set a = 4;
select a;

set schema sys;

start transaction;
commit;

start transaction;
rollback;

set transaction;
rollback;

