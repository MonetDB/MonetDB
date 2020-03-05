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
select cast(y as date) from x; --error, cannot cast
select cast(y as time) from x; --We throw error, but PostgreSQL doesn't
select cast(y as timestamp) from x; --error, cannot cast
insert into x values (null, null);
select cast(x as date) from x; --error, cannot cast
select cast(x as time) from x;
select cast(x as timestamp) from x; --error, cannot cast
select cast(y as date) from x; --error, cannot cast
select cast(y as time) from x; --We throw error, but PostgreSQL doesn't
select cast(y as timestamp) from x; --error, cannot cast
drop table x;

create table x (x time, y date, z timestamp);
insert into x values (null, null, null);
select cast(x as interval second) from x; --We throw error, but PostgreSQL doesn't
select cast(x as interval month) from x; --We throw error, but PostgreSQL doesn't
select cast(y as interval second) from x; --error, cannot cast
select cast(y as interval month) from x; --error, cannot cast
select cast(z as interval second) from x; --error, cannot cast
select cast(z as interval month) from x; --error, cannot cast
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

select cast(true as interval second); --error, not possible
select cast(true as interval month); --error, not possible

select substring('abc' from 1 for null);
select substring('abc' from null for 2);
select substring('abc' from null for null);

select length(myblob), octet_length(myblob), length(mystr), octet_length(mystr) 
from (values (cast(null as blob), cast(null as char(32)))) as my(myblob, mystr);
select md5(null);
