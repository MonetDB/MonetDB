CREATE TABLE tnc (name VARCHAR(10));
INSERT INTO tnc VALUES ('test');

CREATE FUNCTION bottom()
RETURNS TABLE (name string)
BEGIN
	RETURN select name from tnc;
END;

CREATE FUNCTION leaf1()
RETURNS TABLE (name string)
BEGIN
	RETURN select name from bottom()
	UNION  select name from bottom()
	UNION  select name from bottom()
	UNION  select name from bottom();
END;

CREATE FUNCTION leaf2()
RETURNS TABLE (name string)
BEGIN
	RETURN select name from leaf1()
	UNION  select name from leaf1()
	UNION  select name from leaf1()
	UNION  select name from leaf1();
END;

CREATE FUNCTION leaf3()
RETURNS TABLE (name string)
BEGIN
	RETURN select name from leaf2()
	UNION  select name from leaf2()
	UNION  select name from leaf2()
	UNION  select name from leaf2();
END;

SELECT count(*) FROM bottom();
SELECT count(*) FROM leaf1();
SELECT count(*) FROM leaf2();
SELECT count(*) FROM leaf3();

DROP FUNCTION leaf3;
DROP FUNCTION leaf2;
DROP FUNCTION leaf1;
DROP FUNCTION bottom;
DROP TABLE tnc;
