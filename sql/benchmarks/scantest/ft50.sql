-- uses table t50 (see ct50.sql)
--
--  USAGE
--    SQL> @ft50
--    SQL> exec ft50(10)
--

CREATE OR REPLACE
PROCEDURE ft50 (rows INTEGER) IS
	x 		NUMBER := 0.01;
BEGIN
	FOR i IN 1..rows LOOP
		INSERT INTO t50 (Id,v1) VALUES (i, x);
 		x := x + 0.01;
	END LOOP;
END ft50;
.

-- now execute program
/
exec ft50(100000)
