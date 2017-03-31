-- first create a table for all basic data types and fill it with some data rows (including duplicate rows)
create table all_types (
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

insert into all_types values (true, 10, 10000, 1000000,
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
	'www.monetdb.org/Documentation/Manuals/SQLreference/BuiltinTypes',
	'www.monetdb.org/Documentation/Manuals/SQLreference/URLtype',
	'ae106ad4-81fd-4f1a-85e8-5efface60da4');

insert into all_types values (false, -10, -10000, -1000000,
	-10000000000, -1e30, -1e20, -1, -123456789, -12345.678, -3.1415, -3.1415,
	-3.1415, date '2005-04-15', interval '-2' year, interval '-18' month,
	interval '-3' month, interval '-20' day, interval '-30' hour,
	interval '-2000' minute, interval '-100000' second, interval '-10' hour,
	interval '-100' minute, interval '-2000' second, interval '-10' minute,
	interval '-100' second, interval '-10' second,
	timestamp '1988-07-15 07:30', timestamp '1988-07-15 07:30',
	timestamp '1988-07-15 07:30', timestamp '1988-07-15 07:30',
	time '06:30', time '06:30', time '06:30', time '06:30',
	blob '01234567', blob '01234567',
	'0123456', '0123456', 'A', 'Avarchar', 'A012345678',
	'120.0.0.0', '120.120.120.129',
	'{"A": -123}', '{"B": -456}',
	'https://www.monetdb.org/Documentation/Manuals/SQLreference/BuiltinTypes',
	'https://www.monetdb.org/Documentation/Manuals/SQLreference/URLtype',
	'76236890-f4e2-4d4f-a02b-ec7a02c3cb50');

select * from all_types;

-- add same rows again to create duplicate rows
insert into all_types select * from all_types;

select * from all_types;

-- ALTER TABLE all_types SET READ ONLY;

select name, schema_id, type, system, commit_action, access, query from _tables where name = 'all_types';

-- now add ordered indexes for each column (to check all types).
-- synthese the create ordered index commands:
select 'create ordered index "oidx_'||name||'" on all_types ("'||name||'");' as stmt from _columns where table_id in (select id from _tables where name = 'all_types') order by number;

create ordered index "oidx_boolean" on all_types ("boolean");
create ordered index "oidx_tinyint" on all_types ("tinyint");
create ordered index "oidx_smallint" on all_types ("smallint");
create ordered index "oidx_int" on all_types ("int");
create ordered index "oidx_bigint" on all_types ("bigint");
create ordered index "oidx_double" on all_types ("double");
create ordered index "oidx_real" on all_types ("real");
create ordered index "oidx_decimal" on all_types ("decimal");
create ordered index "oidx_decimal9" on all_types ("decimal9");
create ordered index "oidx_decimal83" on all_types ("decimal83");
create ordered index "oidx_float" on all_types ("float");
create ordered index "oidx_float9" on all_types ("float9");
create ordered index "oidx_float83" on all_types ("float83");
create ordered index "oidx_date" on all_types ("date");
create ordered index "oidx_iY" on all_types ("iY");
create ordered index "oidx_iYM" on all_types ("iYM");
create ordered index "oidx_iM" on all_types ("iM");
create ordered index "oidx_id" on all_types ("id");
create ordered index "oidx_idh" on all_types ("idh");
create ordered index "oidx_idm" on all_types ("idm");
create ordered index "oidx_ids" on all_types ("ids");
create ordered index "oidx_ih" on all_types ("ih");
create ordered index "oidx_ihm" on all_types ("ihm");
create ordered index "oidx_ihs" on all_types ("ihs");
create ordered index "oidx_im" on all_types ("im");
create ordered index "oidx_ims" on all_types ("ims");
create ordered index "oidx_is" on all_types ("is");
create ordered index "oidx_timestamp" on all_types ("timestamp");
create ordered index "oidx_timestamp5" on all_types ("timestamp5");
create ordered index "oidx_timestampzone" on all_types ("timestampzone");
create ordered index "oidx_timestamp5zone" on all_types ("timestamp5zone");
create ordered index "oidx_time" on all_types ("time");
create ordered index "oidx_time5" on all_types ("time5");
create ordered index "oidx_timezone" on all_types ("timezone");
create ordered index "oidx_time5zone" on all_types ("time5zone");
-- next data types are not supported in ordered index
create ordered index "oidx_blob" on all_types ("blob");
create ordered index "oidx_blob100" on all_types ("blob100");
create ordered index "oidx_clob" on all_types ("clob");
create ordered index "oidx_clob100" on all_types ("clob100");
create ordered index "oidx_character" on all_types ("character");
create ordered index "oidx_varchar100" on all_types ("varchar100");
create ordered index "oidx_character10" on all_types ("character10");
create ordered index "oidx_inet" on all_types ("inet");
create ordered index "oidx_inet9" on all_types ("inet9");
create ordered index "oidx_json" on all_types ("json");
create ordered index "oidx_json10" on all_types ("json10");
create ordered index "oidx_url" on all_types ("url");
create ordered index "oidx_url55" on all_types ("url55");
create ordered index "oidx_uuid" on all_types ("uuid");

-- synthese the select commands with order by ASC:
select 'select "'||name||'" from all_types order by "'||name||'" ASC;' as stmt from _columns where table_id in (select id from _tables where name = 'all_types') order by number;

select "boolean" from all_types order by "boolean" ASC;
select "tinyint" from all_types order by "tinyint" ASC;
select "smallint" from all_types order by "smallint" ASC;
select "int" from all_types order by "int" ASC;
select "bigint" from all_types order by "bigint" ASC;
select "double" from all_types order by "double" ASC;
select "real" from all_types order by "real" ASC;
select "decimal" from all_types order by "decimal" ASC;
select "decimal9" from all_types order by "decimal9" ASC;
select "decimal83" from all_types order by "decimal83" ASC;
select "float" from all_types order by "float" ASC;
select "float9" from all_types order by "float9" ASC;
select "float83" from all_types order by "float83" ASC;
select "date" from all_types order by "date" ASC;
select "iY" from all_types order by "iY" ASC;
select "iYM" from all_types order by "iYM" ASC;
select "iM" from all_types order by "iM" ASC;
select "id" from all_types order by "id" ASC;
select "idh" from all_types order by "idh" ASC;
select "idm" from all_types order by "idm" ASC;
select "ids" from all_types order by "ids" ASC;
select "ih" from all_types order by "ih" ASC;
select "ihm" from all_types order by "ihm" ASC;
select "ihs" from all_types order by "ihs" ASC;
select "im" from all_types order by "im" ASC;
select "ims" from all_types order by "ims" ASC;
select "is" from all_types order by "is" ASC;
select "timestamp" from all_types order by "timestamp" ASC;
select "timestamp5" from all_types order by "timestamp5" ASC;
select "timestampzone" from all_types order by "timestampzone" ASC;
select "timestamp5zone" from all_types order by "timestamp5zone" ASC;
select "time" from all_types order by "time" ASC;
select "time5" from all_types order by "time5" ASC;
select "timezone" from all_types order by "timezone" ASC;
select "time5zone" from all_types order by "time5zone" ASC;
select "blob" from all_types order by "blob" ASC;
select "blob100" from all_types order by "blob100" ASC;
select "clob" from all_types order by "clob" ASC;
select "clob100" from all_types order by "clob100" ASC;
select "character" from all_types order by "character" ASC;
select "varchar100" from all_types order by "varchar100" ASC;
select "character10" from all_types order by "character10" ASC;
select "inet" from all_types order by "inet" ASC;
select "inet9" from all_types order by "inet9" ASC;
select "json" from all_types order by "json" ASC;
select "json10" from all_types order by "json10" ASC;
select "url" from all_types order by "url" ASC;
select "url55" from all_types order by "url55" ASC;
select "uuid" from all_types order by "uuid" ASC;

-- synthese the select commands with order by DESC:
select 'select "'||name||'" from all_types order by "'||name||'" DESC;' as stmt from _columns where table_id in (select id from _tables where name = 'all_types') order by number;

select "boolean" from all_types order by "boolean" DESC;
select "tinyint" from all_types order by "tinyint" DESC;
select "smallint" from all_types order by "smallint" DESC;
select "int" from all_types order by "int" DESC;
select "bigint" from all_types order by "bigint" DESC;
select "double" from all_types order by "double" DESC;
select "real" from all_types order by "real" DESC;
select "decimal" from all_types order by "decimal" DESC;
select "decimal9" from all_types order by "decimal9" DESC;
select "decimal83" from all_types order by "decimal83" DESC;
select "float" from all_types order by "float" DESC;
select "float9" from all_types order by "float9" DESC;
select "float83" from all_types order by "float83" DESC;
select "date" from all_types order by "date" DESC;
select "iY" from all_types order by "iY" DESC;
select "iYM" from all_types order by "iYM" DESC;
select "iM" from all_types order by "iM" DESC;
select "id" from all_types order by "id" DESC;
select "idh" from all_types order by "idh" DESC;
select "idm" from all_types order by "idm" DESC;
select "ids" from all_types order by "ids" DESC;
select "ih" from all_types order by "ih" DESC;
select "ihm" from all_types order by "ihm" DESC;
select "ihs" from all_types order by "ihs" DESC;
select "im" from all_types order by "im" DESC;
select "ims" from all_types order by "ims" DESC;
select "is" from all_types order by "is" DESC;
select "timestamp" from all_types order by "timestamp" DESC;
select "timestamp5" from all_types order by "timestamp5" DESC;
select "timestampzone" from all_types order by "timestampzone" DESC;
select "timestamp5zone" from all_types order by "timestamp5zone" DESC;
select "time" from all_types order by "time" DESC;
select "time5" from all_types order by "time5" DESC;
select "timezone" from all_types order by "timezone" DESC;
select "time5zone" from all_types order by "time5zone" DESC;
select "blob" from all_types order by "blob" DESC;
select "blob100" from all_types order by "blob100" DESC;
select "clob" from all_types order by "clob" DESC;
select "clob100" from all_types order by "clob100" DESC;
select "character" from all_types order by "character" DESC;
select "varchar100" from all_types order by "varchar100" DESC;
select "character10" from all_types order by "character10" DESC;
select "inet" from all_types order by "inet" DESC;
select "inet9" from all_types order by "inet9" DESC;
select "json" from all_types order by "json" DESC;
select "json10" from all_types order by "json10" DESC;
select "url" from all_types order by "url" DESC;
select "url55" from all_types order by "url55" DESC;
select "uuid" from all_types order by "uuid" DESC;

-- add same rows again to create more duplicate rows
insert into all_types select * from all_types;

select * from all_types order by 11,12,13,14,15,16,17,18,19,20,1,2,3,4,5,6,7,8,9,10;

--cleanup
-- synthese the drop index commands:
select 'drop index "oidx_'||name||'";' as stmt from _columns where table_id in (select id from _tables where name = 'all_types') order by number;

drop index "oidx_boolean";
drop index "oidx_tinyint";
drop index "oidx_smallint";
drop index "oidx_int";
drop index "oidx_bigint";
drop index "oidx_double";
drop index "oidx_real";
drop index "oidx_decimal";
drop index "oidx_decimal9";
drop index "oidx_decimal83";
drop index "oidx_float";
drop index "oidx_float9";
drop index "oidx_float83";
drop index "oidx_date";
drop index "oidx_iY";
drop index "oidx_iYM";
drop index "oidx_iM";
drop index "oidx_id";
drop index "oidx_idh";
drop index "oidx_idm";
drop index "oidx_ids";
drop index "oidx_ih";
drop index "oidx_ihm";
drop index "oidx_ihs";
drop index "oidx_im";
drop index "oidx_ims";
drop index "oidx_is";
drop index "oidx_timestamp";
drop index "oidx_timestamp5";
drop index "oidx_timestampzone";
drop index "oidx_timestamp5zone";
drop index "oidx_time";
drop index "oidx_time5";
drop index "oidx_timezone";
drop index "oidx_time5zone";

drop table all_types cascade;

