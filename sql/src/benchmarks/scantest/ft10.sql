-- uses table t10 (see ct10.sql)
--
--  USAGE
--    SQL> @ft10
--    SQL> exec ft10(10)
--

CREATE OR REPLACE
PROCEDURE ft10 (rows INTEGER) IS
	x 		NUMBER := 0.01;
BEGIN
	FOR i IN 1..rows LOOP
		INSERT INTO t10 (Id,v1) VALUES (i, x);
 		x := x + 0.01;
	END LOOP;
END ft10;
.

-- now execute program
/
exec ft10(100000)
