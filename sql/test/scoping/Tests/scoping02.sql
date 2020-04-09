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

DROP TABLE atest;
