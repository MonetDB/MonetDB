--TODO update rel_dump for schemas on variables

declare i integer;
set i = 1234;

create table tmp1(i integer, s string);
insert into tmp1 values(1,'hello'),(2,'world');
select i from tmp1;

select sys.i, i from tmp1; --we declare variables in a schema, so to reference them we add the schema
	-- 1234 1
	-- 1234 2

declare table tmp2(i integer, s string); --the same for declared tables
insert into tmp2 values(3,'another'),(4,'test');

select tmp1.i, tmp2.i from sys.tmp1, sys.tmp2;
	-- 1 3
	-- 1 4
	-- 2 3
	-- 2 4

SELECT MAX(i) FROM tmp1; --Table columns have precedence over global variables
	-- 2

-- Create a function to test scoping order
CREATE OR REPLACE FUNCTION tests_scopes(i INT) RETURNS INT 
BEGIN
	DECLARE i int;
	SET i = 1;
	IF i = 0 THEN
		RETURN i;
	ELSE
		RETURN sys.i;
	END IF;
END;

SELECT tests_scopes(vals) FROM (VALUES (0),(2),(3)) AS vals(vals);
	-- 0 --Function parameters have precedence over declared variables, either local or global
	-- 1 --Local variables have precedence over global ones
	-- 1

DROP TABLE tmp1;
DROP TABLE tmp2;
DROP FUNCTION tests_scopes(INT);
------------------------------------------------------------------------------
with a(a) as (select 1), a(a) as (select 2) select 1; --error, variable a already declared
------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION scoping(input INT) RETURNS INT 
BEGIN
	DECLARE x int;
	SET x = 1;
	IF input = 2 THEN
		DECLARE x int;
		SET x = 2;
	ELSE
		IF input = 3 THEN
			SET x = 3;
		END IF;
	END IF;
	RETURN x;
END;

SELECT scoping(vals) FROM (VALUES (1),(2),(3)) AS vals(vals);
	-- 1
	-- 1 MonetDB outputs this one as 2, but the inner x shouldn't override the value of outer x variable
	-- 3

DROP FUNCTION scoping(INT);
------------------------------------------------------------------------------
-- A table returning function or view to list the session's variables
select "name", schemaname, "type", currentvalue, accessmode from sys.vars();
------------------------------------------------------------------------------
-- Some syntax to allow users to view GDK variables (we will discuss this later)...
--GRANT WRITE ACCESS to gdk_debug TO PUBLIC;
