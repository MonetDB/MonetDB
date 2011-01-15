-- uses table t1 (see ct1.sql)
--
--  USAGE
--    SQL> @ft1
--    SQL> exec ft1(10)
--

CREATE OR REPLACE
PROCEDURE ft1 (rows INTEGER) IS
	x 		NUMBER := 0.01;
BEGIN
	FOR i IN 1..rows LOOP
		INSERT INTO t1 VALUES (i, x);
 		x := x + 0.01;
	END LOOP;
END ft1;
.

-- now execute program
/
exec ft1(100000)
