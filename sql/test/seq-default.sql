START TRANSACTION;

create sequence test_seq as integer;
create table seqdeftest (ts timestamp, i integer default next value for test_seq);
insert into seqdeftest(ts) values ('2005-09-23 12:34:26.736');
insert into seqdeftest(ts) values ('2005-09-23 12:34:26.736');
insert into seqdeftest(ts) values ('2005-09-23 12:34:26.736');
insert into seqdeftest(ts) values ('2005-09-23 12:34:26.736');
insert into seqdeftest(ts) values ('2005-09-23 12:34:26.736');
insert into seqdeftest(ts) values ('2005-09-23 12:34:26.736');
select * from seqdeftest;

drop table seqdeftest;
drop sequence test_seq;

create table seqdeftest (
	d date,
	id serial,
	count int auto_increment,
	bla int generated always as identity (
		start with 100 increment by 2 no minvalue maxvalue 1000
		cache 2 cycle)
);
insert into seqdeftest(d) values ('2005-10-01');
insert into seqdeftest(d) values ('2005-10-01');
insert into seqdeftest(d) values ('2005-10-01');
insert into seqdeftest(d) values ('2005-10-01');
insert into seqdeftest(d) values ('2005-10-01');
insert into seqdeftest(d) values ('2005-10-01');
select * from seqdeftest;

ROLLBACK;
