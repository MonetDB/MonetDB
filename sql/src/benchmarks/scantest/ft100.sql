-- uses table t100 (see ct100.sql)
--
--  USAGE
--    SQL> @ft100
--    SQL> exec ft100(10)
--

CREATE OR REPLACE
PROCEDURE ft100 (rows INTEGER) IS
	x 		NUMBER := 0.01;
BEGIN
	FOR i IN 1..rows LOOP
		INSERT INTO t100 (Id,v1) VALUES (i, x);
 		x := x + 0.01;
	END LOOP;
END ft100;
.

-- now execute program
/
exec ft100(100000)
