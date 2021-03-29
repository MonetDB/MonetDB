-- ##### get comparison function and operators #####
-- https://www.monetdb.org/Documentation/SQLReference/FunctionsAndOperators/ComparisonFunctionsOperators
SELECT 5 = 5 as tru;
SELECT NULL = NULL as nul;
SELECT 5 > 5 as fals;
SELECT NULL > NULL as nul;
SELECT 5 IS NULL as fals;
SELECT NULL IS NULL as tru;
SELECT 5 IS NOT NULL as tru;
SELECT NULL IS NOT NULL as fals;

select 2 < 5 as t1, 2 > 5 as f1;
select 2 <= 5 as t1, 2 >= 5 as f1;
select 2 = 5 as f1, 2 <> 5 as t1;
select 2 != 5 as t1;
-- Error: Unexpected symbol (!)

select "<"('aa', 'ab') as tru;
select ">"('aa', 'ab') as fals;
select "<="('aa', 'ab') as tru;
select ">="('aa', 'ab') as fals;
select "="('aa', 'ab') as fals;
select "<>"('aa', 'ab') as tru;

select "between"('ab', 'aa', 'ac', false, false, false, false, false) as tru;
select "between"('ab', 'aa', 'ac') as tru;
-- Error: SELECT: no such operator 'between'

select "in"('aa', 'ab') as tru;
-- on Oct2020: Error: SELECT: no such binary operator 'in(char,char)'
-- Error: Implementation for function calc.in not found

select ifthenelse(('a' = 'b'), 1, 2) as two;

select ifnull('aa', 'b') as aa;
-- SELECT: no such binary operator 'ifnull(char,char)'
-- alternatives
select ifthenelse(('aa' IS NULL), cast('b' as varchar(2)), 'aa') as aa;
select coalesce('aa', 'b') as aa;


select ifnull('aa', 'b') as aa;
-- SELECT: no such binary operator 'ifnull(char,char)'
select ifnull(null, 'b') as b;
-- SELECT: no such binary operator 'ifnull(char,char)'
select ifnull(null, null) as nul;
-- SELECT: no such binary operator 'ifnull(any,any)'
select ifnull('aa', null) as nul;
-- SELECT: no such binary operator 'ifnull(char,char)'
select ifnull(true, null) as nul;
-- SELECT: no such binary operator 'ifnull(boolean,boolean)'

-- See: https://www.monetdb.org/bugzilla/show_bug.cgi?id=6933
drop table if exists t6933;
create table t6933 (i int, c varchar(8));
insert into t6933 values (1, 'aa1'), (2, null), (null, 'bb2'), (null, null);

select i, c, ifnull(i, 2*3) as "ifnull(i,2*3)", ifnull(c, 'has null') as "ifnull(i,'x')", ifnull(i, c) as "ifnull(i,c)" from t6933;
-- Error: SELECT: no such binary operator 'ifnull(int,tinyint)'
select c, i, ifnull(c, i) as "ifnull(c,i)" from t6933;
-- Error: SELECT: no such binary operator 'ifnull(varchar,int)'
select c, i, ifnull(c, cast(i as char)) as "ifnull(c,i)" from t6933;
-- Error: SELECT: no such binary operator 'ifnull(varchar,char)'
-- explain select i, c, ifnull(i, c) as "ifnull(i,c)" from t6933;
-- Error: SELECT: no such binary operator 'ifnull(int,varchar)'

select i, c, coalesce(i, 2*3) as "ifnull(i,2*3)", coalesce(c, 'has null') as "ifnull(i,'x')", coalesce(i, c) as "ifnull(i,c)" from t6933;
/*
i	c	ifnull(i,2*3)	ifnull(i,'x')	ifnull(i,c)
1	aa1	1	aa1	1
2	<null>	2	has null	2
<null>	bb2	6	bb2	bb2
<null>	<null>	6	has null	<null>
*/

select i, c, coalesce(i, 2*3) as "coalesce(i,2*3)", coalesce(c, 'has null') as "coalesce(i,'x')", coalesce(i, c) as "coalesce(i,c)" from t6933;

select c, i, coalesce(c, i) as "ifnull(c,i)" from t6933;
/*
c	i	ifnull(c,i)
aa1	1	aa1
<null>	2	2
bb2	<null>	bb2
<null>	<null>	<null>
*/
select c, i, coalesce(c, cast(i as char)) as "ifnull(c,i)" from t6933;
/*
c	i	ifnull(c,i)
aa1	1	aa1
<null>	2	2
bb2	<null>	bb2
<null>	<null>	<null>
*/
select i, c, coalesce(i, c) as "ifnull(i,c)" from t6933;

select i, c, case when i IS NULL then c else i end as "ifnull(i,c)" from t6933;
/*
i	c	ifnull(i,c)
1	aa1	1
2	<null>	2
<null>	bb2	bb2
<null>	<null>	<null>
*/
select i, c, case when i IS NULL then c else i end as "ifnull(i,c)" from t6933;

select i, c, case when i IS NULL then c else cast(i as char) end as "ifnull(i,c)" from t6933;
/*
i	c	ifnull(i,c)
1	aa1	1
2	<null>	2
<null>	bb2	bb2
<null>	<null>	<null>
*/

select i, c, case when i IS NULL then c else cast(i as char) end as "ifnull(i,c)" from t6933;

select c, i, case when c IS NULL then i else c end as "ifnull(c,i)" from t6933;
/*
c	i	ifnull(c,i)
aa1	1	aa1
<null>	2	2
bb2	<null>	bb2
<null>	<null>	<null>
*/

select c, i, case when c IS NULL then i else c end as "ifnull(c,i)" from t6933;

drop table if exists t6933;


select isnull('aa') as fals;

select not_like('abc', '%b%') as fals;
select not_like('abc', '_b%') as fals;
select not_like('abc', '_b_') as fals;
select not_like('abc', '_c_') as tru;

select not_like('%b%', 'abc') as tru;
select not_like('_b%', 'abc') as tru;
select not_like('_b_', 'abc') as tru;
select not_like('_c_', 'abc') as tru;

select not_like('abc', '%b%', '%') as fals;
-- on Oct2020: Error: Illegal argument: (I)LIKE pattern must not end with escape character
-- #client1:!ERROR:MALException:pcre.sql2pcre:operation failed
select not_like('abc', '%b%c', '%') as tru;

select not_like('abc', '_b%', '%') as fals;
-- on Oct2020: Error: Illegal argument: (I)LIKE pattern must not end with escape character
-- #client1:!ERROR:MALException:pcre.sql2pcre:operation failed
select not_like('abc', '_b%c', '%') as fals;
-- false on Oct2020
-- #client1:!ERROR:MALException:pcre.sql2pcre:operation failed

select not_like('abc', '_b_', '%') as fals;
select not_like('abc', '_c_', '%') as tru;

select not_like('abc', '%b%', '_') as fals;
select not_like('abc', '_b%', '_') as tru;
select not_like('abc', '_b_', '_') as tru;
-- on Oct2020: Error: Illegal argument: (I)LIKE pattern must not end with escape character
-- #client1:!ERROR:MALException:pcre.sql2pcre:operation failed
select not_like('abc', '_c_', '_') as tru;
-- on Oct2020: Error: Illegal argument: (I)LIKE pattern must not end with escape character
-- #client1:!ERROR:MALException:pcre.sql2pcre:operation failed

select not_like('abc', '%b%', '#') as fals;
select not_like('abc', '_b%', '#') as fals;
select not_like('abc', '_b_', '#') as fals;
select not_like('abc', '_c_', '#') as tru;

select not_like('ab#c', '%b%', '#') as fals;
select not_like('ab#c', '_b%', '#') as fals;
select not_like('ab#c', '_b_', '#') as tru;
select not_like('ab#c', '_c_', '#') as tru;

select not_like('abc', '%b#%', '#') as tru;
select not_like('abc', '_b#%', '#') as tru;
select not_like('a_bc', '_#_b_', '#') as fals;
select not_like('abc', '_c#_', '#') as tru;


select not_like('aabbccdd', '%bc\\_d%', '\\') as tru;

select not_like('aabbccdd', '%bc_d%') as fals;
select not_like('aabbccdd', '%bc\\_d%', '\\') as tru;


select not_like('abc', '%B%') as tru;
select not_like('abc', '_B%') as tru;
select not_like('abc', '_B_') as tru;

select not_ilike('abc', '%B%') as fals;
select not_ilike('abc', '_B%') as fals;
select not_ilike('abc', '_B_') as fals;
select not_ilike('abc', '_C_') as tru;

select not_ilike('a_bc', '_#_B_', '#') as fals;

select not_ilike('aabbccdd', '%bc_d%') as fals;
select not_ilike('aabbccdd', '%bc\\_d%', '\\') as tru;

select like('aabbccdd', '%bc_d%') as tru;
-- Error: syntax error, unexpected LIKE in: "select like"
select "like"('aabbccdd', '%bc_d%') as tru;
-- true on Oct2020
-- Error: SELECT: no such binary operator 'like(char,char)'
select "ilike"('aabbccdd', '%bc_d%') as tru;
-- true on Oct2020
-- Error: SELECT: no such binary operator 'ilike(char,char)'

select sql_exists('abc') as tru;
select sql_not_exists('abc') as fals;


-- coalesce(arg1, arg2, ...) Returns the first non-NULL value in the list, or NULL if there are no non-NULL values. At least two parameters must be passed.
--select * from sys.functions where name in ('coalesce');
-- 1 row for Oct2020. Note both func and mod values are empty strings
-- no rows

select coalesce(1, 'b', null);
-- 1
select coalesce('b', null, 2);
-- b on Oct2020
-- Error: conversion of string 'b' to type bte failed.
select coalesce(null, 2, 'b');
-- 2
select coalesce(null, 2, 'b', 4);
-- 2 on Oct2020
-- Error: conversion of string 'b' to type bte failed.
select coalesce(null, 'b', 4);
-- b on Oct2020
-- Error: conversion of string 'b' to type bte failed.
select coalesce('ac', 'dc');
-- ac
select coalesce(null, 'ac', 'dc');
-- ac
select coalesce(null, null, 'ac', 'dc');
-- ac
select coalesce(null, null, null, 'ac', 'dc');
-- ac
select coalesce(null, null, null, null, 'ac', 'dc');
-- ac
select coalesce(null, null, null, null, null, 'ac', 'dc');
-- ac
select coalesce(null, null, null, null, null, null, 'ac', 'dc');
-- ac
--plan select coalesce(null, null, null, null, null, null, 'ac', 'dc');
-- 3 rows
--explain select coalesce(null, null, null, null, null, null, 'ac', 'dc');
-- 32 rows

-- select name, coalesce(query, name), query from sys._tables;
-- select name, coalesce(query) from sys._tables;
-- in Oct2020: Error: syntax error, unexpected ')', expecting OR or ',' in: "select name, coalesce(query)"
-- Error: syntax error, unexpected ')', expecting ',' in: "select name, coalesce(query)"
-- plan select name, coalesce(query, query), query from sys._tables;
-- 3 rows
-- plan select name, coalesce(query, query) from sys._tables;
-- 3 rows


-- nullif(arg1, arg2) Returns NULL if expr1 = expr2 is true, otherwise returns expr1. This is the same as CASE WHEN expr1 = expr2 THEN NULL ELSE expr1 END.
--select * from sys.functions where name in ('nullif');
-- 1 row for Oct2020. Note both func and mod values are empty strings
-- no rows

select nullif('dc');
-- in Oct2020: Error: syntax error, unexpected ')', expecting OR or ',' in: "select nullif('dc')"
-- Error: syntax error, unexpected ')' in: "select nullif('dc')"
select nullif('ac', 'dc');
-- ac
select nullif('ac', 'dc', 'de');
-- in Oct2020: Error: syntax error, unexpected ',', expecting ')' or OR in: "select nullif('ac', 'dc',"
-- Error: syntax error, unexpected ',' in: "select nullif('ac', 'dc',"

select nullif('ac', 'dc');
-- ac
select CASE WHEN 'ac' = 'dc' THEN NULL ELSE 'ac' END;
-- ac

select nullif(null, 'dc');
-- null
select CASE WHEN null = 'dc' THEN NULL ELSE null END;
-- null

select nullif('ac', null);
-- ac
select CASE WHEN 'ac' = null THEN NULL ELSE 'ac' END;
-- ac

select nullif(null, null);
-- null
select CASE WHEN null = null THEN NULL ELSE null END;
-- null

select nullif('ac', 'ac');
-- null
select CASE WHEN 'ac' = 'ac' THEN NULL ELSE 'ac' END;
-- null

select nullif(9, 9);
-- null
select CASE WHEN 9 = 9 THEN NULL ELSE 9 END;
-- null

plan select nullif(null, null);
plan select CASE WHEN null = null THEN NULL ELSE null END;
plan select nullif('ac', 'ac');
plan select CASE WHEN 'ac' = 'ac' THEN NULL ELSE 'ac' END;
plan select nullif(9, 9);
plan select CASE WHEN 9 = 9 THEN NULL ELSE 9 END;


select 'db' between 'abc' and 'db' as tru;
select 'db' not between 'abc' and 'db' as fals;
select not 'db' between 'abc' and 'db' as fals;

select 'db' between SYMMETRIC 'abc' and 'db' as tru;
select 'db' between SYMMETRIC 'db' and 'abc' as tru;
-- true on Oct2020
-- Error: min or max operator on types char char missing
select 'db' not between SYMMETRIC 'abc' and 'db' as fals;
select 'db' not between SYMMETRIC 'db' and 'abc' as fals;
-- false on Oct2020
-- Error: min or max operator on types char char missing

select 'a' IS DISTINCT FROM 'b' as tru;
-- Error: syntax error, unexpected DISTINCT, expecting sqlNULL or NOT in: "select 'a' is distinct"

select 'a' IS NULL as fals;
select 'a' IS NOT NULL as tru;

select 'a' ISNULL as fals;
-- Error: syntax error, unexpected AS, expecting SCOLON in: "select 'a' isnull as"
select 'a' ISNOTNULL as tru;
-- Error: syntax error, unexpected AS, expecting SCOLON in: "select 'a' isnotnull as"

select ('a' = 'b') IS TRUE;
-- Error: syntax error, unexpected BOOL_TRUE, expecting sqlNULL or NOT in: "select ('a' = 'b') is true"
select ('a' = 'b') IS NOT TRUE;
-- Error: syntax error, unexpected BOOL_TRUE, expecting sqlNULL in: "select ('a' = 'b') is not true"
select ('a' = 'b') IS FALSE;
-- Error: syntax error, unexpected BOOL_FALSE, expecting sqlNULL or NOT in: "select ('a' = 'b') is false"
select ('a' = 'b') IS NOT FALSE;
-- Error: syntax error, unexpected BOOL_FALSE, expecting sqlNULL or NOT in: "select ('a' = 'b') is not false"
select ('a' = 'b') IS UNKNOWN;
-- Error: syntax error, unexpected IDENT, expecting sqlNULL or NOT in: "select ('a' = 'b') is unknown"
select ('a' = 'b') IS NOT UNKNOWN;
-- Error: syntax error, unexpected IDENT, expecting sqlNULL or NOT in: "select ('a' = 'b') is not unknown"

