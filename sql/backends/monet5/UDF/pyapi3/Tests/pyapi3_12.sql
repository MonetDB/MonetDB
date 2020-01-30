# Some additional tests for multiprocessing
# Multiprocessing in the WHERE clause can fail if the SEQBASE of the BATs are not set correctly because the BATs are split up
# Returning a numpy.object array can fail because numpy.object is essentially an array of pointers, so we if copy just the pointers into shared memory/mmap it breaks
# We fix this by internally converting the data back into a 'normal' Numpy array of type STRING or INT or whatever (depending on the objects)
START TRANSACTION;

CREATE TABLE rval(i integer);
INSERT INTO rval VALUES (1),(2),(3),(4),(-1),(0);

# PYTHON_MAP test in WHERE
CREATE FUNCTION pyapi12(i integer,z integer) returns boolean language PYTHON_MAP
{
	return(numpy.greater(i,z))
};
SELECT * FROM rval WHERE pyapi12(i,2);
DROP FUNCTION pyapi12;


# Return NPY_OBJECT test
CREATE FUNCTION pyapi12(i integer,z integer) returns string language PYTHON_MAP
{
	return(numpy.array(['Hello'] * len(i), dtype=object))
};
SELECT pyapi12(i,2) FROM rval;
DROP FUNCTION pyapi12;


DROP TABLE rval;


ROLLBACK;
