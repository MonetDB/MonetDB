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

drop table seqdeftest;

-- create a user created sequence with name pattern 'seq_####' similar to the system generated sequences as in the above table seqdeftest
CREATE SEQUENCE seq_5700 AS INTEGER;
CREATE TABLE kvk (
        id                INTEGER       NOT NULL       DEFAULT next value for seq_5700,
        kvk               BIGINT,
        bedrijfsnaam      VARCHAR(256),
        CONSTRAINT kvk_id_pkey PRIMARY KEY (id)
);
insert into kvk(kvk, bedrijfsnaam) values (1, 'ones');
insert into kvk(kvk, bedrijfsnaam) values (2, 'toos');
select * from kvk;

-- use the same sequence also in another table
CREATE TABLE kvk_cp (
        id                INTEGER       NOT NULL       DEFAULT next value for seq_5700,
        kvk               BIGINT,
        bedrijfsnaam      VARCHAR(256),
        CONSTRAINT kvk_cp_id_pkey PRIMARY KEY (id)
);
insert into kvk_cp(kvk, bedrijfsnaam) values (3, 'dries');
insert into kvk_cp(kvk, bedrijfsnaam) values (4, 'viers');
select * from kvk_cp;

-- when dropping the tables the user defined sequence should *not* be dropped implicitly
drop TABLE kvk;

insert into kvk_cp(kvk, bedrijfsnaam) values (5, 'vijfs');
select * from kvk_cp;
drop TABLE kvk_cp;

-- the user created sequence should not have been implicitly dropped at this point
drop SEQUENCE seq_5700;

ROLLBACK;
