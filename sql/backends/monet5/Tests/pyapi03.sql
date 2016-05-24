# Use MonetDB/Python to filter data by creating a simple filter that is identical to 'i > z' in SQL
START TRANSACTION;

CREATE TABLE rval(i integer);
INSERT INTO rval VALUES (1),(2),(3),(4),(-1),(0);

CREATE FUNCTION pyapi03(i integer,z integer) returns boolean language P {return(numpy.greater(i,z))};
SELECT * FROM rval WHERE pyapi03(i,2);
DROP FUNCTION pyapi03;
DROP TABLE rval;


ROLLBACK;
