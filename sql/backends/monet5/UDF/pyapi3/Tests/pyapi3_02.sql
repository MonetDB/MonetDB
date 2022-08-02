# Use MonetDB/Python for some simple SELECT() operations
START TRANSACTION;

CREATE TABLE rval(i integer,j integer);
INSERT INTO rval VALUES (1,4), (2,3), (3,2), (4,1);

CREATE FUNCTION pyapi02(i integer,j integer,z integer) returns integer
language P
{
    x = i * sum(j) * z;
    return(x);
};

SELECT pyapi02(i,j,2) FROM rval;
DROP FUNCTION pyapi02;

CREATE FUNCTION pyapi02(i integer) returns integer
language P
{
	return numpy.min(i)
};

SELECT pyapi02(i) FROM rval;

DROP TABLE rval;

ROLLBACK;

