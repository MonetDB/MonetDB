-- ##### get logical function and operators #####
-- https://www.monetdb.org/Documentation/SQLReference/FunctionsAndOperators/LogicalFunctionsOperators
/*
all(arg_1 boolean, arg_2 boolean, arg_3 boolean)
and(arg_1 boolean, arg_2 boolean)
any(arg_1 boolean, arg_2 boolean, arg_3 boolean)
not(arg_1 boolean)
or(arg_1 boolean, arg_2 boolean)
xor(arg_1 boolean, arg_2 boolean)
*/

drop table if exists b;
create table b (a boolean, b boolean);
insert into b values (true, true);
insert into b values (true, false);
insert into b values (true, null);
insert into b values (false, true);
insert into b values (false, false);
insert into b values (false, null);
insert into b values (null, true);
insert into b values (null, false);
insert into b values (null, null);
select * from b;
-- 9 rows for Oct2020

select b.*, a and b, a or b, not a, a IS NULL, not a IS NULL, a IS NOT NULL from b;
select b.*, a and b, "and"(a,b), a or b, "or"(a,b), not a, "xor"(a,b) from b;

select not(true), not(null), not(false), "and"(true, false), "or"(true, false), "xor"(true, false), "all"(true, false, true), "any"(true, false, true);
plan select not(true), not(null), not(false), "and"(true, false), "or"(true, false), "xor"(true, false), "all"(true, false, true), "any"(true, false, true);

select a, b, coalesce(a, b) as "coalesce(a,b)" from b;


create view b3 as select a,b,true as c from b union all select a,b,false as c from b union all select a,b,null as c from b;
select * from b3;
-- 27 rows for Oct2020

select b.*, "all"(a,b,a) as aba, "all"(a,b,b) as abb, "all"(null,a,b) as NULLab, "all"(null,b,a) as NULLba from b;
select b3.*, "all"(a,b,c) as abc from b3;
select "all"(null,a,b) as abF from b;

select b.*, "any"(a,a,a), "any"(a,a,b), "any"(a,b,a), "any"(a,b,b), "any"(b,a,a), "any"(b,a,b), "any"(b,b,a), "any"(b,b,b) from b;
select b.*, "any"(a,b,a) as aba, "any"(a,b,b) as abb from b;
select b3.*, "any"(a,b,c) as abc from b3;

-- Comparison operators on booleans!!
select a, b, a = b as "a = b", a < b as "a < b", a > b as "a > b", a <= b as "a <= b", a >= b as "a >= b", a <> b as "a <> b" from b order by a desc, b desc;
/*
-- 9 rows:
a	b	a = b	a < b	a > b	a <= b	a >= b	a <> b
true 	true 	true 	false	false	true 	true 	false
true 	false	false	false	true 	false	true 	true
true 	<null>	<null>	<null>	<null>	<null>	<null>	<null>
false	true 	false	true 	false	true 	false	true
false	false	true 	false	false	true 	true 	false
false	<null>	<null>	<null>	<null>	<null>	<null>	<null>
<null>	true 	<null>	<null>	<null>	<null>	<null>	<null>
<null>	false	<null>	<null>	<null>	<null>	<null>	<null>
<null>	<null>	<null>	<null>	<null>	<null>	<null>	<null>
*/

-- cleanup
drop view if exists b3;
drop table if exists b;

