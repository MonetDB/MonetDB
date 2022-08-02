
# Loopback query tests
START TRANSACTION;

# First test: simply duplicate a table using a loopback query by loading the table into Python using _conn
CREATE TABLE pyapi09table(i integer);
INSERT INTO pyapi09table VALUES (1), (2), (3), (4);

CREATE FUNCTION pyapi09() returns TABLE(i integer)
language P
{
    res = _conn.execute('SELECT i FROM pyapi09table;')
    return res
};

SELECT * FROM pyapi09();
DROP FUNCTION pyapi09;
DROP TABLE pyapi09table;

# Second test: use data from a different table in computation
CREATE TABLE pyapi09table(i integer);
INSERT INTO pyapi09table VALUES (1), (2), (3), (4);
CREATE TABLE pyapi09multiplication(i integer);
INSERT INTO pyapi09multiplication VALUES (3);

CREATE FUNCTION pyapi09(i integer) returns integer
language P
{
    res = _conn.execute('SELECT i FROM pyapi09multiplication;')
    return res['i'] * i
};

SELECT pyapi09(i) FROM pyapi09table; #multiply by 3
UPDATE pyapi09multiplication SET i=10;
SELECT pyapi09(i) FROM pyapi09table; #multiply by 10

DROP FUNCTION pyapi09;
DROP TABLE pyapi09table;
DROP TABLE pyapi09multiplication;

#Third test: Store python object in table and load it in later function

CREATE FUNCTION pyapi09create() returns TABLE(s BLOB)
language P
{
    import pickle
    result = numpy.arange(100000)
    return pickle.dumps(result)
};

# Create the table containing the numpy array
CREATE TABLE pyapi09objects AS SELECT * FROM pyapi09create() WITH DATA;

# Load the data from the table
CREATE FUNCTION pyapi09load() returns TABLE(i INTEGER)
language P
{
    import pickle
    res = _conn.execute('SELECT s FROM pyapi09objects;')
    array = pickle.loads(res['s'][0])
    print(array)
    return array[:10]
};

SELECT * FROM pyapi09load();

DROP FUNCTION pyapi09create;
DROP FUNCTION pyapi09load;
DROP TABLE pyapi09objects;

# Fourth test: Error in query, 
# Load the data from the table
CREATE FUNCTION pyapi09() returns TABLE(i INTEGER)
language P
{
    res = _conn.execute('SELECT * FROM unknown_table;')
    return numpy.arange(10)
};

SELECT * FROM pyapi09();
ROLLBACK;

START TRANSACTION;

# Fifth test: Error in query, but we capture it, does it ruin our transaction?
# Load the data from the table
CREATE FUNCTION pyapi09() returns TABLE(i INTEGER)
language P
{
    try: res = _conn.execute('SELECT * FROM unknown_table;')
    except: pass
    return numpy.arange(10)
};

SELECT * FROM pyapi09();
SELECT 1; # yes it does...

ROLLBACK;
