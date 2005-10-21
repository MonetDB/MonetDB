-- this code currently triggers a multitude of bugs...


create sequence test_seq as integer;
create table test (ts timestamp, i integer default next value for test_seq);
insert into test(ts) values '2005-09-23 12:34:26.736';
insert into test(ts) values '2005-09-23 12:34:26.736';
insert into test(ts) values '2005-09-23 12:34:26.736';
insert into test(ts) values '2005-09-23 12:34:26.736';
insert into test(ts) values '2005-09-23 12:34:26.736';
insert into test(ts) values '2005-09-23 12:34:26.736';
select * from test;

drop sequence test_seq;
drop table test;

create table test (
	d date,
	id serial,
	count int auto_increment,
	bla int generated always as identity (
		start with 100 increment by 2 minvalue 1 maxvalue 1000 cycle)
);
insert into test(d) values '2005-10-01';
insert into test(d) values '2005-10-01';
insert into test(d) values '2005-10-01';
insert into test(d) values '2005-10-01';
insert into test(d) values '2005-10-01';
insert into test(d) values '2005-10-01';
select * from test;
