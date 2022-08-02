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

create table "typestest" (
        "boolean" boolean,
        "tinyint" tinyint,
        "smallint" smallint,
        "int" int,
        "bigint" bigint,
        "double" double,
        "real" real,
        "decimal" decimal,
        "decimal9" decimal(9),
        "decimal83" decimal(8,3),
        "float" float,
        "float9" float(9),
        "float83" float(8,3),
        "date" date,
        "iY" interval year,
        "iYM" interval year to month,
        "iM" interval month,
        "id" interval day,
        "idh" interval day to hour,
        "idm" interval day to minute,
        "ids" interval day to second,
        "ih" interval hour,
        "ihm" interval hour to minute,
        "ihs" interval hour to second,
        "im" interval minute,
        "ims" interval minute to second,
        "is" interval second,
        "timestamp" timestamp,
        "timestamp5" timestamp(5),
        "timestampzone" timestamp with time zone,
        "timestamp5zone" timestamp(5) with time zone,
        "time" time,
        "time5" time(5),
        "timezone" time with time zone,
        "time5zone" time(5) with time zone,
        "blob" blob,
        "blob100" blob(100),
        "clob" clob,
        "clob100" clob(100),
        "character" character,
        "varchar100" character varying(100),
        "character10" character(10),
        "inet"   inet,
        "inet9"  inet(9),
        "json"   json,
        "json10" json(10),
        "url"    url,
        "url55"  URL(55),
        "uuid"   uuid
);
insert into "typestest" values (true, 10, 10000, 1000000,
	10000000000, 1e30, 1e20, 1, 123456789, 12345.678, 3.1415, 3.1415,
	3.1415, date '2009-04-15', interval '2' year, interval '18' month,
	interval '3' month, interval '20' day, interval '30' hour,
	interval '2000' minute, interval '100000' second, interval '10' hour,
	interval '100' minute, interval '2000' second, interval '10' minute,
	interval '100' second, interval '10' second,
	timestamp '1995-07-15 07:30', timestamp '1995-07-15 07:30',
	timestamp '1995-07-15 07:30', timestamp '1995-07-15 07:30',
	time '07:30', time '07:30', time '07:30', time '07:30',
	blob '123456', blob '123456',
	'123456', '123456', 'x', 'varchar', '0123456789',
        '127.0.0.0', '127.127.127.255',
        '{"a": 123}', '{"b": 456}',
        'https://www.monetdb.org/Documentation/Manuals/SQLreference/BuiltinTypes',
        'https://www.monetdb.org/Documentation/Manuals/SQLreference/URLtype',
        'ae106ad4-81fd-4f1a-85e8-5efface60da4');

create table keytest1 (
	key1 int,
	key2 int,
	primary key (key1, key2)
);
create table keytest2 (
	key1 int,
	key2 int,
	foreign key (key1, key2) references keytest1 (key1, key2)
);
insert into keytest1 values (0, 0);
insert into keytest1 values (0, 1);
insert into keytest2 values (0, 0);
insert into keytest2 values (0, 1);

