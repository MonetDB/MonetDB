-- this code currently triggers a multitude of bugs...


create sequence test_seq as integer;
create table test (ts timestamp default now, i integer default next value for test_seq);
insert into test ;
insert into test ;
insert into test ;
insert into test ;
insert into test ;
insert into test ;
select * from test;

drop sequence test_seq;
drop table test;

create table test (
	d date default cast(now() as date),
	id serial,
	count int auto_increment
);
insert into test ;
insert into test ;
insert into test ;
insert into test ;
insert into test ;
insert into test ;
select * from test;
