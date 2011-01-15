-- uses table t25 (see ct25.sql)
--
--  USAGE
--    SQL> @ft25
--    SQL> exec ft25(10)
--

CREATE OR REPLACE
PROCEDURE ft25 (rows INTEGER) IS
	x 		NUMBER := 0.01;
BEGIN
	FOR i IN 1..rows LOOP
		INSERT INTO t25 (Id,v1) VALUES (i, x);
 		x := x + 0.01;
	END LOOP;
END ft25;
.

-- now execute program
/
exec ft25(100000)
