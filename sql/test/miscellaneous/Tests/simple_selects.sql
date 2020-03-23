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
insert into x values (1, 1);
select cast(x as date) from x; --error, cannot cast
select cast(x as time) from x;
select cast(x as timestamp) from x; --error, cannot cast
select cast(x as real) from x;
select cast(x as double) from x;
select cast(x as decimal) from x;
select cast(y as date) from x; --error, cannot cast
select cast(y as time) from x; --We throw error, but PostgreSQL doesn't
select cast(y as timestamp) from x; --error, cannot cast
select cast(y as real) from x;
select cast(y as double) from x;
select cast(y as decimal) from x;

insert into x values (null, null);
select cast(x as date) from x; --error, cannot cast
select cast(x as time) from x;
select cast(x as timestamp) from x; --error, cannot cast
select cast(x as real) from x;
select cast(x as double) from x;
select cast(x as decimal) from x;
select cast(y as date) from x; --error, cannot cast
select cast(y as time) from x; --We throw error, but PostgreSQL doesn't
select cast(y as timestamp) from x; --error, cannot cast
select cast(y as real) from x;
select cast(y as double) from x;
select cast(y as decimal) from x;
drop table x;

create table x (x time, y date, z timestamp, w real, a double, b decimal);
insert into x values (null, null, null, null, null, null);
select cast(x as interval second) from x; --We throw error, but PostgreSQL doesn't
select cast(x as interval month) from x; --We throw error, but PostgreSQL doesn't
select cast(y as interval second) from x; --error, cannot cast
select cast(y as interval month) from x; --error, cannot cast
select cast(z as interval second) from x; --error, cannot cast
select cast(z as interval month) from x; --error, cannot cast
select cast(w as interval second) from x;
select cast(w as interval month) from x;
select cast(a as interval second) from x;
select cast(a as interval month) from x;
select cast(b as interval second) from x;
select cast(b as interval month) from x;
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

select x like x, x ilike x, x not like x, x not ilike x from x;
	-- NULL NULL NULL NULL
	-- True True False False

select x1.x from x x1 inner join x x2 on x1.x not like x2.x; --empty

select i from (values (1),(2),(3),(NULL)) as integers(i) where not cast(i as varchar(32)) like null; --empty

drop table x;

create table x (x int null not null); --error, multiple null constraints
create table x (a int default '1' GENERATED ALWAYS AS IDENTITY); --error, multiple default values

DECLARE myvar bigint;
SET myvar = (SELECT COUNT(*) FROM sequences);
create table x (a int GENERATED ALWAYS AS IDENTITY);
alter table x alter a set default 1; --ok, remove sequence
SELECT CAST(COUNT(*) - myvar AS BIGINT) FROM sequences; --the total count, cannot change
drop table x;

SET myvar = (SELECT COUNT(*) FROM sequences);
create table x (a int GENERATED ALWAYS AS IDENTITY);
alter table x alter a drop default; --ok, remove sequence
SELECT CAST(COUNT(*) - myvar AS BIGINT) FROM sequences; --the total count, cannot change
drop table x;

SELECT 1, 2 INTO myvar; --error, number of variables don't match

declare table x (a int);
declare table x (c int); --error table x already declared
drop table if exists x;
