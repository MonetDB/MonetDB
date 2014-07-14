--
-- ERRORS
--

-- bad in postquel, but ok in postsql
select 1;


--
-- UNSUPPORTED STUFF
 
-- doesn't work 
-- attachas nonesuch
--
-- doesn't work 
-- notify pg_class
--

--
-- SELECT
 
-- missing relation name 
select;

-- no such relation 
select * from nonesuch;

-- missing target list
select from pg_database;
-- bad name in target list
select nonesuch from pg_database;
-- bad attribute name on lhs of operator
select * from pg_database where nonesuch = pg_database.datname;

-- bad attribute name on rhs of operator
select * from pg_database where pg_database.datname = nonesuch;


-- bad select distinct on syntax, distinct attribute missing
select distinct on (foobar) from pg_database;


-- bad select distinct on syntax, distinct attribute not in target list
select distinct on (foobar) * from pg_database;


--
-- DELETE
 
-- missing relation name (this had better not wildcard!) 
delete from;

-- no such relation 
delete from nonesuch;


--
-- DROP
 
-- missing relation name (this had better not wildcard!) 
drop table;

-- no such relation 
drop table nonesuch;


--
-- ALTER TABLE
 
-- relation renaming 

-- missing relation name 
alter table rename;

-- no such relation 
alter table nonesuch rename to newnonesuch;

-- no such relation 
alter table nonesuch rename to stud_emp;

-- conflict 
alter table stud_emp rename to aggtest;

-- self-conflict 
alter table stud_emp rename to stud_emp;


-- attribute renaming 

-- no such relation 
alter table nonesuchrel rename column nonesuchatt to newnonesuchatt;

-- no such attribute 
alter table emp rename column nonesuchatt to newnonesuchatt;

-- conflict 
alter table emp rename column salary to manager;

-- conflict 
alter table emp rename column salary to oid;


--
-- TRANSACTION STUFF
 
-- not in a xact 
abort;

-- not in a xact 
COMMIT;


--
-- CREATE AGGREGATE

-- sfunc/finalfunc type disagreement 
create aggregate newavg2 (sfunc = int4pl,
			  basetype = integer,
			  stype = integer,
			  finalfunc = int2um,
			  initcond = '0');

-- left out basetype
create aggregate newcnt1 (sfunc = int4inc,
			  stype = integer,
			  initcond = '0');


--
-- DROP INDEX
 
-- missing index name 
drop index;

-- bad index name 
drop index 314159;

-- no such index 
drop index nonesuch;


--
-- DROP AGGREGATE
 
-- missing aggregate name 
drop aggregate;

-- missing aggregate type
drop aggregate newcnt1;

-- bad aggregate name 
drop aggregate 314159 (int);

-- bad aggregate type
drop aggregate newcnt (nonesuch);

-- no such aggregate 
drop aggregate nonesuch (integer);

-- no such aggregate for type
drop aggregate newcnt (float);


--
-- DROP FUNCTION
 
-- missing function name 
drop function ();

-- bad function name 
drop function 314159();

-- no such function 
drop function nonesuch();


--
-- DROP TYPE
 
-- missing type name 
drop type;

-- bad type name 
drop type 314159;

-- no such type 
drop type nonesuch;


--
-- DROP OPERATOR
 
-- missing everything 
drop operator;

-- bad operator name 
drop operator equals;

-- missing type list 
drop operator ===;

-- missing parentheses 
drop operator integer, integer;

-- missing operator name 
drop operator (integer, integer);

-- missing type list contents 
drop operator === ();

-- no such operator 
drop operator === (integer);

-- no such operator by that name 
drop operator === (integer, integer);

-- no such type1 
drop operator = (nonesuch);

-- no such type1 
drop operator = ( , integer);

-- no such type1 
drop operator = (nonesuch, integer);

-- no such type2 
drop operator = (integer, nonesuch);

-- no such type2 
drop operator = (integer, );


--
-- DROP RULE
 
-- missing rule name 
drop rule;

-- bad rule name 
drop rule 314159;

-- no such rule 
drop rule nonesuch on noplace;

-- these postquel variants are no longer supported
drop tuple rule nonesuch;
drop instance rule nonesuch on noplace;
drop rewrite rule nonesuch;

--
-- Check that division-by-zero is properly caught.
--

select 1/0;

select 1::bigint/0;

select 1/0::bigint;

select 1::smallint/0;

select 1/0::smallint;

select 1::numeric/0;

select 1/0::numeric;

select 1::double/0;

select 1/0::double;

select 1::float/0;

select 1/0::float;


--
-- Test psql's reporting of syntax error location
--

xxx;

CREATE foo;

CREATE TABLE ;

CREATE TABLE
\g

INSERT INTO foo VALUES(123) foo;

INSERT INTO 123
VALUES(123);

INSERT INTO foo 
VALUES(123) 123
;

-- with a tab
CREATE TABLE foo
  (id integer UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY,
	id3 INTEGER NOT NUL,
   id4 integer UNIQUE NOT NULL, id5 TEXT UNIQUE NOT NULL);

-- long line to be truncated on the left
CREATE TABLE foo(id integer UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL, 
id4 integer UNIQUE NOT NULL, id5 TEXT UNIQUE NOT NULL);

-- long line to be truncated on the right
CREATE TABLE foo(
id3 INTEGER NOT NUL, id4 integer UNIQUE NOT NULL, id5 TEXT UNIQUE NOT NULL, id integer UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY);

-- long line to be truncated both ways
CREATE TABLE foo(id integer UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL, id4 integer UNIQUE NOT NULL, id5 TEXT UNIQUE NOT NULL);

-- long line to be truncated on the left, many lines
CREATE
TEMPORARY
TABLE 
foo(id integer UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL, 
id4 integer 
UNIQUE 
NOT 
NULL, 
id5 TEXT 
UNIQUE 
NOT 
NULL)
;

-- long line to be truncated on the right, many lines
CREATE 
TEMPORARY
TABLE 
foo(
id3 INTEGER NOT NUL, id4 integer UNIQUE NOT NULL, id5 TEXT UNIQUE NOT NULL, id integer UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY)
;

-- long line to be truncated both ways, many lines
CREATE 
TEMPORARY
TABLE 
foo
(id 
integer 
UNIQUE NOT NULL, idx integer UNIQUE NOT NULL, idy integer UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL, id4 integer UNIQUE NOT NULL, id5 TEXT UNIQUE NOT NULL, 
idz integer UNIQUE NOT NULL, 
idv integer UNIQUE NOT NULL);

-- more than 10 lines...
CREATE 
TEMPORARY
TABLE 
foo
(id 
integer 
UNIQUE 
NOT 
NULL
, 
idm
integer 
UNIQUE 
NOT 
NULL,
idx integer UNIQUE NOT NULL, idy integer UNIQUE NOT NULL, id2 TEXT NOT NULL PRIMARY KEY, id3 INTEGER NOT NUL, id4 integer UNIQUE NOT NULL, id5 TEXT UNIQUE NOT NULL, 
idz integer UNIQUE NOT NULL, 
idv 
integer 
UNIQUE 
NOT 
NULL);
