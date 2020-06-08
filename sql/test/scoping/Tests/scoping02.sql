CREATE TABLE atest (a int);
INSERT INTO atest VALUES (1);

CREATE OR REPLACE FUNCTION iambroken() RETURNS TABLE(a int) 
BEGIN
	DECLARE TABLE atest (a int);
	INSERT INTO x VALUES (2); --error, x doesn't exist
	RETURN x;
END;

SELECT a FROM atest;
	-- 1
SELECT a FROM iambroken(); --error

CREATE OR REPLACE FUNCTION iambroken() RETURNS TABLE(a int) 
BEGIN
	DECLARE TABLE sys.atest (a int); --error, declared tables inside functions don't have a schema
	RETURN SELECT a FROM x;
END;

CREATE OR REPLACE FUNCTION iambroken() RETURNS TABLE(a int) 
BEGIN
	DECLARE sys.atest int; --error, declared variables inside functions don't have a schema
	RETURN x;
END;

DROP TABLE atest;

CREATE TABLE sys.mytable (a int);
INSERT INTO sys.mytable VALUES (1);

CREATE OR REPLACE FUNCTION scoping01(i INT) RETURNS INT
BEGIN
	DECLARE TABLE mytable (a int);
	INSERT INTO mytable VALUES (2);
	IF i = 1 THEN
		RETURN SELECT a FROM sys.mytable;
	ELSE
		RETURN SELECT a FROM mytable;
	END IF;
END;

SELECT scoping01(vals) FROM (VALUES (1), (2)) AS vals(vals);
	-- 1
	-- 2

DROP FUNCTION scoping01(INT);
DROP TABLE sys.mytable;

CREATE OR REPLACE FUNCTION iambroken() RETURNS INT
BEGIN
	DECLARE TABLE mytable (a int);
	RETURN SELECT a FROM sys.mytable; --error table sys.mytable doesn't exist
END;

CREATE OR REPLACE FUNCTION scoping02(i INT) RETURNS INT 
BEGIN
	IF i = 1 THEN
		DECLARE i int; --valid, i is redeclared on another scope
		RETURN i;
	END IF;
	RETURN i;
END;

SELECT scoping02(vals) FROM (VALUES (1), (2)) AS vals(vals);
	-- NULL
	-- 2

DROP FUNCTION scoping02(INT);

CREATE TABLE "sys" ("current_schema" string);
INSERT INTO "sys" VALUES ('astring');

SELECT "sys"."current_schema" FROM "sys";
	-- astring

DROP TABLE "sys";

SELECT * from sys.var() where "name" not in ('debug', 'cache', 'last_id', 'current_timezone');

set "current_schema" = null; --error, cannot be null
set "sys"."current_timezone" = null; --error, cannot be null
