
# Loopback query tests for mapped functions
START TRANSACTION;

# Use data from a different table in computation
CREATE TABLE pyapi25table(i integer);
INSERT INTO pyapi25table VALUES (1), (2), (3), (4);
CREATE TABLE pyapi25multiplication(i integer);
INSERT INTO pyapi25multiplication VALUES (3);

CREATE FUNCTION pyapi25(i integer) returns integer
language PYTHON_MAP
{
    res = _conn.execute('SELECT i FROM pyapi25multiplication;')
    return res['i'] * i
    return i
};

SELECT pyapi25(i) FROM pyapi25table; #multiply by 3
UPDATE pyapi25multiplication SET i=10;
SELECT pyapi25(i) FROM pyapi25table; #multiply by 10

DROP FUNCTION pyapi25;
DROP TABLE pyapi25table;
DROP TABLE pyapi25multiplication;

# Create and update tables in the connection
CREATE TABLE pyapi25table(i integer);
INSERT INTO pyapi25table VALUES (1), (2), (3), (4);

CREATE FUNCTION pyapi25(i integer) returns integer
language PYTHON
{
    _conn.execute('CREATE TABLE mytable(i INTEGER);')
    _conn.execute('INSERT INTO mytable VALUES (1), (2), (3), (4);')
    return i
};

SELECT pyapi25(i) FROM pyapi25table;

CREATE FUNCTION pyapi25map(i integer) returns integer
language PYTHON_MAP
{
    _conn.execute('UPDATE mytable SET i=i*10;')
    return i
};

SELECT * FROM mytable; # 1,2,3,4
SELECT pyapi25map(i) FROM pyapi25table;
SELECT * FROM mytable; # 10000, 20000, 30000, 40000 (*10 for every thread, 4 threads because there are 4 entries in pyapi25table)

# store stuff in a parallel query
# we compute the mean in parallel, then store the mean of every thread in the table
CREATE TABLE pyapi25medians(mean DOUBLE);

CREATE FUNCTION pyapi25randomtable() returns TABLE(d DOUBLE) LANGUAGE PYTHON
{
    numpy.random.seed(33)
    return numpy.random.rand(1000000)
};

CREATE FUNCTION pyapi25mediancompute(d DOUBLE) RETURNS DOUBLE
language PYTHON_MAP
{
    mean = numpy.mean(d)
    _conn.execute('INSERT INTO pyapi25medians (mean) VALUES (%g);' % mean)
    return 1
};

# to verify that the output is correct, we check if the mean stored in pyapi25medians is within an epsilon of the actual mean of the data
CREATE FUNCTION pyapi25checker(d DOUBLE) RETURNS BOOL LANGUAGE PYTHON {
    actual_mean = numpy.mean(d)
    numpy.random.seed(33)
    expected_mean = numpy.mean(numpy.random.rand(1000000))
    if numpy.abs(expected_mean - actual_mean) < 0.1:
        print("Great success!")
        return(True)
    else:
        print("Incorrect mean %g: expected %g" % (actual_mean, expected_mean))
        print("Values:", d)
        return(False)
};

CREATE TABLE randomtable AS SELECT * FROM pyapi25randomtable() WITH DATA;

SELECT pyapi25mediancompute(d) FROM randomtable;
SELECT pyapi25checker(mean) FROM pyapi25medians;

# test error in parallel SQL query

CREATE FUNCTION pyapi25errortable() returns TABLE(d DOUBLE) LANGUAGE PYTHON_MAP
{
    _conn.execute('SELECT * FROM HOPEFULLYNONEXISTANTTABLE;')
    return 1
};

SELECT * FROM pyapi25errortable();

ROLLBACK;



