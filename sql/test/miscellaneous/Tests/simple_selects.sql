select 1 where false;
select 1 where true;
select (select 1 where false);
select (select 1 where true);
select (select (select 1 where true) where false);
select (select (select 1 where false) where true);
select (select (select 1 where true) where true);
select (select (select 1 where false) where false);

select count(*) having -1 > 0;
select cast(sum(42) as bigint) group by 1;
select cast(sum(42) as bigint) limit 2;
select cast(sum(42) as bigint) having 42>80;

select 1 having false;
select 1 having true;
select -NULL;

create table x (x interval second, y interval month);
insert into x values (interval '1' second, interval '1' month);
select cast(x as date) from x; --error, cannot cast
select cast(x as time) from x;
select cast(x as timestamp) from x; --error, cannot cast
select cast(x as real) from x; --error, cannot cast
select cast(x as double) from x; --error, cannot cast
select cast(x as decimal) from x; --error, cannot cast
select cast(y as date) from x; --error, cannot cast
select cast(y as time) from x; --We throw error, but PostgreSQL doesn't
select cast(y as timestamp) from x; --error, cannot cast
select cast(y as real) from x; --error, cannot cast
select cast(y as double) from x; --error, cannot cast
select cast(y as decimal) from x; --error, cannot cast

insert into x values (null, null);
select cast(x as date) from x; --error, cannot cast
select cast(x as time) from x;
select cast(x as timestamp) from x; --error, cannot cast
select cast(x as real) from x; --error, cannot cast
select cast(x as double) from x; --error, cannot cast
select cast(x as decimal) from x; --error, cannot cast
select cast(y as date) from x; --error, cannot cast
select cast(y as time) from x; --We throw error, but PostgreSQL doesn't
select cast(y as timestamp) from x; --error, cannot cast
select cast(y as real) from x; --error, cannot cast
select cast(y as double) from x; --error, cannot cast
select cast(y as decimal) from x; --error, cannot cast
drop table x;

create table x (x time, y date, z timestamp, w real, a double, b decimal);
insert into x values (null, null, null, null, null, null);
select cast(x as interval second) from x; --We throw error, but PostgreSQL doesn't
select cast(x as interval month) from x; --We throw error, but PostgreSQL doesn't
select cast(y as interval second) from x; --error, cannot cast
select cast(y as interval month) from x; --error, cannot cast
select cast(z as interval second) from x; --error, cannot cast
select cast(z as interval month) from x; --error, cannot cast
select cast(w as interval second) from x; --error, cannot cast
select cast(w as interval month) from x; --error, cannot cast
select cast(a as interval second) from x; --error, cannot cast
select cast(a as interval month) from x; --error, cannot cast
select cast(b as interval second) from x; --error, cannot cast
select cast(b as interval month) from x; --error, cannot cast
drop table x;

select difference('foobar', 'oobar'), difference(NULL, 'oobar'), difference('foobar', NULL), difference(NULL, NULL),
       editdistance('foobar', 'oobar'), editdistance(NULL, 'oobar'), editdistance('foobar', NULL), editdistance(NULL, NULL), 
       editdistance2('foobar', 'oobar'), editdistance2(NULL, 'oobar'), editdistance2('foobar', NULL), editdistance2(NULL, NULL),
       similarity('foobar', 'oobar'), similarity(NULL, 'oobar'), similarity('foobar', NULL), similarity(NULL, NULL),
       levenshtein('foobar', 'oobar'), levenshtein(NULL, 'oobar'), levenshtein('foobar', NULL), levenshtein(NULL, NULL);
select avg(10), avg(NULL),
       patindex('o', 'foo'), patindex(NULL, 'foo'), patindex('o', NULL), patindex('o', NULL), patindex(NULL, NULL),
       "hash"(null);

select "idontexist"."idontexist"(); --error, it doesn't exist
select "idontexist"."idontexist"(1); --error, it doesn't exist
select "idontexist"."idontexist"(1,2); --error, it doesn't exist
select "idontexist"."idontexist"(1,2,3); --error, it doesn't exist
select "idontexist".SUM(1); --error, it doesn't exist
select * from "idontexist"."idontexist"(); --error, it doesn't exist
select * from "idontexist"."idontexist"(1); --error, it doesn't exist
call "idontexist"."idontexist"(); --error, it doesn't exist
call "idontexist"."idontexist"(1); --error, it doesn't exist
select "idontexist"."idontexist"(1) over (); --error, it doesn't exist

select cast(true as interval second); --error, not possible
select cast(true as interval month); --error, not possible
select cast(cast(1 as interval second) as boolean); --error, not possible
select cast(cast(1 as interval month) as boolean); --error, not possible

select cast(null as blob) > cast(null as blob);
	-- NULL

select substring('abc' from 1 for null);
select substring('abc' from null for 2);
select substring('abc' from null for null);

CREATE FUNCTION count(input INT) RETURNS INT BEGIN RETURN SELECT 1; END; --error, ambiguous, there's an aggregate named count with the same parameters
CREATE AGGREGATE sin(input REAL) RETURNS REAL EXTERNAL NAME "mmath"."sin"; --error, ambiguous, there's a function named sin with the same parameters

select length(myblob), octet_length(myblob), length(mystr), octet_length(mystr) 
from (values (cast(null as blob), cast(null as char(32)))) as my(myblob, mystr);
select md5(null);

select 'a' like null, null like 'a', null like null, 'a' ilike null, null ilike 'a', null ilike null,
       'a' not like null, null not like 'a', null not like null, 'a' not ilike null, null not ilike 'a', null not ilike null; --all NULL

create table x (x varchar(32));
insert into x values (null), ('a');

select x like null, null like x, null like null, x ilike null, null ilike x, null ilike null,
       x not like null, null not like x, null not like null, x not ilike null, null not ilike x, null not ilike null from x;
	-- all NULL

select x like null from x;
	-- NULL
	-- NULL

select x like x, x ilike x, x not like x, x not ilike x from x;
	-- NULL NULL NULL NULL
	-- True True False False

select x1.x from x x1 inner join x x2 on x1.x not like x2.x; --empty

select i from (values (1),(2),(3),(NULL)) as integers(i) where not cast(i as varchar(32)) like null; --empty

drop table x;

create table x (x int null not null); --error, multiple null constraints
create table x (a int default '1' GENERATED ALWAYS AS IDENTITY); --error, multiple default values

create table myvar (m bigint);
INSERT INTO myvar VALUES ((SELECT COUNT(*) FROM sequences));
create table x (a int GENERATED ALWAYS AS IDENTITY);
alter table x alter a set default 1; --ok, remove sequence
SELECT CAST(COUNT(*) - (SELECT m FROM myvar) AS BIGINT) FROM sequences; --the total count, cannot change
drop table myvar;
drop table x;

create table myvar (m bigint);
INSERT INTO myvar VALUES ((SELECT COUNT(*) FROM sequences));
create table x (a int GENERATED ALWAYS AS IDENTITY);
alter table x alter a drop default; --ok, remove sequence
SELECT CAST(COUNT(*) - (SELECT m FROM myvar) AS BIGINT) FROM sequences; --the total count, cannot change
drop table myvar;
drop table x;

create function myudf() returns int
begin
declare myvar int;
SELECT 1, 2 INTO myvar; --error, number of variables don't match
return 1;
end;

create table x (a int);
create table x (c int); --error table x already declared
drop table if exists x;

create table myx (a boolean);
create table myy (a interval second);
select * from myx natural full outer join myy; --error, types boolean(1,0) and sec_interval(13,0) are not equal
drop table myx;
drop table myy;

create view iambad as select * from _tables sample 10; --error, sample inside views not supported

set "current_timezone" = null; --error, default global variables cannot be null
set "current_timezone" = interval '111111111' second; --error, value too big
set "current_timezone" = 11111111111111; --can't do it
set "current_schema" = null; --error, default global variables cannot be null

select greatest(null, null);
select sql_min(null, null);

start transaction;
create table tab1(col1 blob default blob '1122');
insert into tab1 values('2233');
select length(col1) from tab1;
insert into tab1 values(null), (null), ('11'), ('2233');
select length(col1) from tab1;
insert into tab1 values(default);
select col1 from tab1;
rollback;

select 'a' like 'a' escape 'a'; --error, like sequence ending with escape character 

select cast(x as interval second) from (values ('1'), (NULL), ('100'), (NULL)) as x(x);
select cast(x as interval month) from (values ('1'), (NULL), ('100'), (NULL)) as x(x);

select cast(92233720368547750 as interval month); --error value to large for a month interval
select cast(92233720368547750 as interval second); --error, overflow in conversion for interval second

start transaction;
CREATE VIEW myv AS
SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, fk.name AS fk_name
  FROM sys.tables AS t, sys.keys AS k, sys.keys AS fk
 WHERE fk.rkey = k.id and k.table_id = t.id
 ORDER BY t.schema_id, t.name, fk.name;
select * from myv limit 1;
rollback;

select * from (select 1 as c0, 2 as c0) as sub;
	-- error, duplicate column names

select *,* from (select 1 as c0, 2 as c0) as sub;
	-- error, duplicate column names

select * from (select 1 as c0, max(k) as c0 from (select 2, 3) tst(k, name) group by name) as sub;
	-- error, duplicate column names

select cast(interval '3' second as clob);
	-- 3.000

select cast(13121 as varchar(2147483647)) || cast(231231 as varchar(2147483647)); --error, too big

select date_to_str(a,'%y/%m/%d') from (values (date '2012-02-11'), (date '2012-02-12'),(date '2012-02-13')) as a(a);

select x as z, 2 from (select 1) as x(x) order by z, z;
	-- 1 2

select x as z, 2 from (select 1) as x(x) group by z, z;
	-- 1 2

plan select x as z, 2 from (select 1) as x(x) group by z, z;

select x as z, y as z from (select 1, 2) as x(x,y) group by z;
	-- error 'z' identifier is ambiguous

select x as z, y as z from (select 1, 2) as x(x,y) order by z;
	-- error 'z' identifier is ambiguous

select 1, null except select 1, null;
	-- empty

select 1, null intersect select 1, null;
	-- 1 NULL

select ifthenelse(false, 'abc', 'abcd'), ifthenelse(false, 1.23, 12.3);
	-- abcd 12.30

start transaction;
create table t1("kk" int);
create table t2("kk" int);
create table t3("tkey" int);

SELECT 1 FROM (((t1 t10 INNER JOIN t2 t20 ON t10."kk" = t20."kk") INNER JOIN t2 t20 ON t10."kk" = t20."kk")
INNER JOIN t3 t31 ON t20."kk" = t31."tkey"); --error, multiple references to relation t20
rollback;

start transaction;
CREATE TABLE "t0" ("c0" int NOT NULL,CONSTRAINT "t0_c0_pkey" PRIMARY KEY ("c0"));
CREATE TABLE "t1" ("c1" int NOT NULL,CONSTRAINT "t1_c1_pkey" PRIMARY KEY ("c1"));
alter table t1 drop constraint "t0_c0_pkey"; --error, constraint "t0_c0_pkey" not available on table t1
rollback;

start transaction;
create or replace function ups() returns int begin if null > 1 then return 1; else return 2; end if; end;
select ups();
	-- 2
create or replace function ups() returns int begin while 1 = 1 do if null is null then return 1; else return 2; end if; end while; end;
select ups();
	-- 1
create or replace function ups() returns int begin declare a int; set a = 2; while a < 2 do if null is null then return 3; else set a = 2; end if; end while; end;
select ups();
	-- 3
create or replace function ups() returns int begin if 1 > 1 then return 1; end if; end; --error, return missing
rollback;

create or replace function ups() returns int begin declare a int; while 1 = 1 do set a = 2; end while; end; --error, return missing

create or replace function ups(v int) returns int begin declare a int; case v when 1 then return 100; when 2 then set a = 2; else return -1; end case; end; --error, return missing

start transaction;
create function "😀"() returns int return 1;
select "😀"();
	-- 1
rollback;

CREATE FUNCTION ups() RETURNS INT
BEGIN
	DECLARE "nononononononononononononononononononononononononononononononono" int;
	RETURN "nononononononononononononononononononononononononononononononono";
END;  -- error for now
select ups();  -- error for now
create function "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"() returns int return 2;  -- error for now
select "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"();  -- error for now
create function "😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀"() returns int return 3;  -- error for now
select "😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀"();  -- error for now
