set schema sys;
set schema json;
set role monetdb;

set schema sys;

start transaction;
commit;

start transaction;
rollback;

set transaction;
rollback;

