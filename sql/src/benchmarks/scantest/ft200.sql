-- uses table t200 (see ct200.sql)
--
--  USAGE
--    SQL> @ft200
--    SQL> exec ft200(10)
--

CREATE OR REPLACE
PROCEDURE ft200 (rows INTEGER) IS
	x 		NUMBER := 0.01;
BEGIN
	FOR i IN 1..rows LOOP
		INSERT INTO t200 (Id,v1) VALUES (i, x);
 		x := x + 0.01;
	END LOOP;
END ft200;
.

-- now execute program
/
exec ft200(100000)
