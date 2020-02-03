# This test case tests variable number of arguments (*) using MonetDB/Python. 
# We can use the parameters by using the _columns() dictionary.
# Note that the parameters are "anonymous" (they are called 'arg1', 'arg2', etc... instead of the input names)

START TRANSACTION;

CREATE FUNCTION pyapi24(*) RETURNS integer LANGUAGE PYTHON
{
    sum = 0
    print(_columns.keys())
    for key in _columns.keys():
        sum += numpy.sum(_columns[key])
    return sum
};

CREATE TABLE pyapi24table(i INTEGER, j INTEGER, k INTEGER, l INTEGER, m INTEGER);
INSERT INTO pyapi24table VALUES (1, 2, 3, 4, 5), (1, 2, 3, 4, 5), (1, 2, 3, 4, 5), (1, 2, 3, 4, 5), (1, 2, 3, 4, 5);

# different columns 
SELECT pyapi24(i) FROM pyapi24table;
SELECT pyapi24(i,j) FROM pyapi24table;
SELECT pyapi24(i,j,k) FROM pyapi24table;
SELECT pyapi24(i,j,k,l) FROM pyapi24table;
SELECT pyapi24(i,j,k,l,m) FROM pyapi24table;

# use the same column over and over
SELECT pyapi24(i,i,i,i,i) FROM pyapi24table;
SELECT pyapi24(i,i,i,i,i,i,i,i,i,i) FROM pyapi24table;
SELECT pyapi24(i,i,i,i,i,i,i,i,i,i,i,i,i,i,i) FROM pyapi24table;
SELECT pyapi24(i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i) FROM pyapi24table;
SELECT pyapi24(i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i) FROM pyapi24table;
#lets go a little crazy (100 columns)
SELECT pyapi24(i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i) FROM pyapi24table;

DROP FUNCTION pyapi24;
DROP TABLE pyapi24table;

#lets try to mix column types


CREATE FUNCTION pyapi24(*) RETURNS DOUBLE LANGUAGE PYTHON
{
    sum = 0
    for key in _columns.keys():
        if _column_types[key] != 'STRING':
            sum += numpy.sum(_columns[key])
        else: 
            sum += 10000 * len(_columns[key]) # strings are worth more points
    return sum
};

CREATE TABLE pyapi24table(i BOOLEAN, j TINYINT, k INTEGER, l DOUBLE, m STRING);
INSERT INTO pyapi24table VALUES (True, 10, 1000, 100.1, 'Hello'), (True, 10, 1000, 100.1, 'Hello'), (True, 10, 1000, 100.1, 'Hello'), (True, 10, 1000, 100.1, 'Hello'), (True, 10, 1000, 100.1, 'Hello');

SELECT pyapi24(i,j,k,l,m) FROM pyapi24table; #55555.5

ROLLBACK;
