create sequence "test_seq" as smallint start with 7 increment by 3 minvalue 5 maxvalue 10 cycle;
create table "test" (
	id integer,
	i smallint default next value for test_seq);
insert into test (id) values (0);
insert into test (id) values (1);
insert into test (id) values (2);
insert into test (id) values (3);
insert into test (id) values (4);
select * from test;
