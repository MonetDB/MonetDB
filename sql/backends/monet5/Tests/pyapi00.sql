# Basic MonetDB/Python test
# We simply return a list of the values 1-10 and store it in a table
START TRANSACTION;

CREATE FUNCTION pyapi00() returns table (d integer)
language P
{
	return(list(range(1,11)))
};

SELECT * FROM pyapi00() AS R WHERE d > 5;
DROP FUNCTION pyapi00;

ROLLBACK;
