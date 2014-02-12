create user test with password testing name "Test User" schema 'default-schema';
create table sys.test ( i int, s string );
grant select on table test to test;

select * from test;
insert into test values (1, 'monetdb');
