CREATE SEQUENCE sys.seq_5700 AS INTEGER;

CREATE TABLE kvk (
        id                INTEGER       NOT NULL       DEFAULT next value for seq_5700,
        kvk               BIGINT,
        bedrijfsnaam      VARCHAR(256),
        adres             VARCHAR(256),
        postcode          VARCHAR(10),
        plaats            VARCHAR(32),
        type              VARCHAR(14),
        kvks              INTEGER,
        sub               INTEGER,
        bedrijfsnaam_size SMALLINT,
        adres_size        SMALLINT,
        website           VARCHAR(255),
        CONSTRAINT kvk_id_pkey PRIMARY KEY (id)
);

insert into  kvk(kvk, bedrijfsnaam, adres, postcode, plaats, type, kvks, sub, bedrijfsnaam_size,  adres_size, website) values (1, 'table1', 'table1', 'table1',  'table1',  'table1',  23, 24, 1, 1, 'table1');
insert into  kvk(kvk, bedrijfsnaam, adres, postcode, plaats, type, kvks, sub, bedrijfsnaam_size,  adres_size, website) values (1, 'table1', 'table1', 'table1',  'table1',  'table1',  23, 24, 1, 1, 'table2');

CREATE TABLE kvk_extra_nieuw (
        kvk          BIGINT,
        bedrijfsnaam VARCHAR(256),
        straat       VARCHAR(256),
        nummer       INTEGER,
        toevoeging   VARCHAR(16),
        postcode     VARCHAR(10),
        plaats       VARCHAR(32),
        website      VARCHAR(128),
        type         VARCHAR(14),
        status       VARCHAR(64),
        kvks         INTEGER,
        sub          INTEGER
);

insert into  kvk_extra_nieuw values (1, 'test', 'test', 10,  'test',  'test',  'test',  'test',  'test',  'test', 10, 11);
insert into  kvk_extra_nieuw values (2, 'test', 'test', 10,  'test',  'test',  'test',  'test',  'test',  'test', 10, 11);
insert into  kvk_extra_nieuw values (3, 'test', 'test', 10,  'test',  'test',  'test',  'test',  'test',  'test', 10, 11);
insert into  kvk_extra_nieuw values (4, 'test', 'test', 10,  'test',  'test',  'test',  'test',  'test',  'test', 10, 11);
insert into  kvk_extra_nieuw values (5, 'test', 'test', 10,  'test',  'test',  'test',  'test',  'test',  'test', 10, 11);

select * from  kvk;
select * from  kvk_extra_nieuw;
update kvk set website = (select distinct kvk_extra_nieuw.website from kvk, kvk_extra_nieuw WHERE
kvk.kvk = kvk_extra_nieuw.kvk and kvk_extra_nieuw.website is not null);

select * from  kvk;

drop table kvk_extra_nieuw;
drop table  kvk;
drop SEQUENCE sys.seq_5700;
