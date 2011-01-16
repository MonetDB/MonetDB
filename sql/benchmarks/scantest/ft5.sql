-- uses table t5 (see ct5.sql)
--
--  USAGE
--    SQL> @ft5
--    SQL> exec ft5(10)
--

CREATE OR REPLACE
PROCEDURE ft5 (rows INTEGER) IS
	x 		NUMBER := 0.01;
BEGIN
	FOR i IN 1..rows LOOP
		INSERT INTO t5 (Id,v1) VALUES (i, x);
 		x := x + 0.01;
	END LOOP;
END ft5;
.

-- now execute program
/
exec ft5(100000)
