create sequence test_seq as integer;
create table test (ts timestamp default now, i integer default next value for test_seq);
insert into test ;
insert into test ;
insert into test ;
insert into test ;
insert into test ;
insert into test ;
select * from test;
