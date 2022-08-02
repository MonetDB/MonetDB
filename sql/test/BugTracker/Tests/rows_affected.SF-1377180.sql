start transaction;
create table test( i integer );
delete from test;
rollback;
