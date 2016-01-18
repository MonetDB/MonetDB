
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
# I should probably find a way to force mtest to run with 8 threads, because the result will differ on machines without exactly 8 cores
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
    _conn.execute('INSERT INTO pyapi25medians (mean) VALUES (' + str(mean) + ');')
    return 1
};

CREATE TABLE randomtable AS SELECT * FROM pyapi25randomtable() WITH DATA;

SELECT pyapi25mediancompute(d) FROM randomtable;
SELECT * FROM pyapi25medians ORDER BY mean;

# test error in parallel SQL query

CREATE FUNCTION pyapi25errortable() returns TABLE(d DOUBLE) LANGUAGE PYTHON_MAP
{
    return _conn.execute('SELECT * FROM HOPEFULLYNONEXISTANTTABLE;')
    return 1
};

SELECT * FROM pyapi25errortable();

ROLLBACK;



