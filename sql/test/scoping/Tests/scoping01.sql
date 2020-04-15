-- temp tables precedence
-- hash lookup for variables
-- TODO transaction management
-- Test variables with different schemas and scoping levels
-- upgrade drop dt_schema
-- drop tables and variables?

declare i integer;
set i = 1234;

create table tmp1(i integer, s string);
insert into tmp1 values(1,'hello'),(2,'world');
select i from tmp1;
	-- 1
	-- 2

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

CREATE OR REPLACE FUNCTION tests_scopes1(i INT) RETURNS INT 
BEGIN
	DECLARE i int; --error, variable redeclaration;
	RETURN i;
END;

SELECT tests_scopes1(vals) FROM (VALUES (1),(2),(3)) AS vals(vals); --will trigger error

-- Create a function to test scoping order
CREATE OR REPLACE FUNCTION tests_scopes2(i INT) RETURNS INT 
BEGIN
	DECLARE j int;
	SET j = i;
	RETURN j;
END;

SELECT tests_scopes2(vals) FROM (VALUES (1),(2),(3)) AS vals(vals);
	-- 1 --Local variables have precedence over global ones
	-- 2 
	-- 3

with a(i) as (select 4) select i from tmp1, a; --error, ambiguous identifier 'i'

DROP TABLE tmp1;
DROP TABLE tmp2;
DROP FUNCTION tests_scopes1(INT);
DROP FUNCTION tests_scopes2(INT);
------------------------------------------------------------------------------
DECLARE "current_schema" string; --error, "current_schema" already declared
DECLARE "sys"."current_schema" string; --error, "current_schema" already declared
with a(a) as (select 1), a(a) as (select 2) select 1; --error, CTE a already declared
with a(a) as (with a(a) as (select 1) select 2) select a from a; --allowed
	-- 2

DECLARE "aux" string;
SET "aux" = (SELECT "optimizer");
SET "optimizer" = 'default_pipe';

CREATE OR REPLACE FUNCTION tests_scopes3(input INT) RETURNS STRING 
BEGIN
	IF input = 1 THEN
		DECLARE "optimizer" string; --allowed
		SET "optimizer" = 'anything';
	END IF;
	RETURN SELECT "optimizer";
END;

SELECT tests_scopes3(0), tests_scopes3(1);
	-- default_pipe default_pipe

SET "optimizer" = (SELECT "aux");
DROP FUNCTION tests_scopes3(INT);
------------------------------------------------------------------------------
create function tests_scopes4() returns int begin declare table y (a int, b int); return select y; end; --error, cannot project a declared table
create function tests_scopes4() returns table (i integer, s string) begin return select tmp2; end; --error, cannot project a declared table

declare table tmp2(i integer, s string); --the same for declared tables
insert into tmp2 values(3,'another'),(4,'test');

create function tests_scopes4() returns table (i integer, s string) begin return tmp2; end; --possible, return the contents of tmp2
select * from tests_scopes4();
	-- 3 another
	-- 4 test

DROP TABLE tmp2; --error dependency on tmp2
DROP FUNCTION tests_scopes4;
DROP TABLE tmp2;
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
	-- 1
	-- 3

DROP FUNCTION scoping(INT);

CREATE OR REPLACE FUNCTION scoping2(input INT) RETURNS INT 
BEGIN
	DECLARE TABLE z (a int);
	INSERT INTO z VALUES (1);
	IF input = 2 THEN
		DECLARE TABLE z (a int);
		INSERT INTO z VALUES (2);
	ELSE
		IF input = 3 THEN
			TRUNCATE z;
			INSERT INTO z VALUES (3);
		END IF;
	END IF;
	RETURN SELECT a FROM z;
END; 

SELECT scoping2(vals) FROM (VALUES (1),(2),(3),(4)) AS vals(vals);
	-- 1
	-- 1
	-- 3
	-- 1

CREATE OR REPLACE FUNCTION scoping2(input INT) RETURNS INT 
BEGIN
	IF input = 2 THEN
		DECLARE TABLE z (a int);
		DECLARE TABLE z (a int); --error, redifinition on the same scope level
	END IF;
	RETURN SELECT a FROM z;
END; 

DROP FUNCTION scoping2(INT);
-----------------------------------------------------------------------------
DECLARE TABLE atest (a int);
INSERT INTO atest VALUES (1);

CREATE OR REPLACE FUNCTION scoping3() RETURNS TABLE(a int) 
BEGIN
	DECLARE TABLE atest (a int); -- allowed, the table atest from scoping3 is unrelated to "atest" from the global scope
	INSERT INTO atest VALUES (2);
	RETURN atest;
END;

SELECT a FROM atest;
	-- 1
SELECT a FROM scoping3();
	-- 2

CREATE OR REPLACE FUNCTION scoping4() RETURNS TABLE(a int)
BEGIN
	DECLARE tableydoesntexist int;
	RETURN tableydoesntexist; --error, no table named "tableydoesntexist" exists
END;

CREATE OR REPLACE FUNCTION scoping4() RETURNS TABLE(a int)
BEGIN
	DECLARE TABLE mytable (a int);
	RETURN mytable, mytable; --error, cannot return two tables (we get an parser error which is also fine)
END;

CREATE OR REPLACE FUNCTION scoping4() RETURNS INT
BEGIN
	RETURN idontexist; --error, no variable named "idontexist" exists
END;

CREATE OR REPLACE FUNCTION scoping4() RETURNS INT
BEGIN
	DECLARE idoexist int;
	RETURN idoexist, idoexist; --error, cannot return two variables  (we get an parser error which is also fine)
END;

CREATE OR REPLACE FUNCTION scoping4() RETURNS INT
BEGIN
	DECLARE TABLE z (a int); 
	RETURN VALUES (z); --error, there's no declared variable z on the scope
END;

DROP TABLE atest;
DROP FUNCTION scoping3;
------------------------------------------------------------------------------
-- A table returning function or view to list the session's variables
--select "name", schemaname, "type", currentvalue, accessmode from sys.vars();
------------------------------------------------------------------------------
-- Some syntax to allow users to view GDK variables (we will discuss this later)...
--GRANT WRITE ACCESS to gdk_debug TO PUBLIC;
