DECLARE TABLE atest (a int);
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
