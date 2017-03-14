# Test 'CREATE OR REPLACE' functionality

START TRANSACTION;
# first create the initial table and function
CREATE TABLE createorreplacetable(i INTEGER);
INSERT INTO createorreplacetable VALUES (2), (4), (6);

# this simple function multiplies elements by 2
CREATE FUNCTION createorreplacefunc(i INTEGER) RETURNS INTEGER LANGUAGE PYTHON {
	return i * 2;
};

# (4), (8), (12)
SELECT createorreplacefunc(i) FROM createorreplacetable;

COMMIT;

START TRANSACTION;
# try to create the function again: this should fail
CREATE FUNCTION createorreplacefunc(i INTEGER) RETURNS INTEGER LANGUAGE PYTHON { return i * 3; };
ROLLBACK;

START TRANSACTION;
# now replace the function with a function that multiplies elements by 3
CREATE OR REPLACE FUNCTION createorreplacefunc(i INTEGER) RETURNS INTEGER LANGUAGE PYTHON {
	return i * 3;
};
# (6), (12), (18)
SELECT createorreplacefunc(i) FROM createorreplacetable;
ROLLBACK;

START TRANSACTION;
DROP FUNCTION createorreplacefunc;
COMMIT;

# aggregates
START TRANSACTION;
CREATE AGGREGATE createorreplaceaggregate(i INTEGER) RETURNS INTEGER LANGUAGE PYTHON {
	return numpy.min(i);
};
# (2)
SELECT createorreplaceaggregate(i) FROM createorreplacetable;

CREATE OR REPLACE AGGREGATE createorreplaceaggregate(i INTEGER) RETURNS INTEGER LANGUAGE PYTHON {
	return numpy.max(i);
};
# (6)
SELECT createorreplaceaggregate(i) FROM createorreplacetable;

ROLLBACK;


START TRANSACTION;
DROP TABLE createorreplacetable;
COMMIT;
