start transaction;
create table test ( str varchar (20) );
insert into test values ( 'test' );
select * from test;
select cast ( str as varchar (2)) from test;
