# Create a table with multiple columns with MonetDB/Python
START TRANSACTION;

CREATE FUNCTION pyapi01(i integer) returns table (i integer, d double)
language P
{
	x = range(1, i + 1)
	y = [42.0] * i
	return([x,y])
};
SELECT i,d FROM pyapi01(42) AS R WHERE i>30;
DROP FUNCTION pyapi01;

ROLLBACK;
