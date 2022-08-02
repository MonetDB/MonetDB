
START TRANSACTION;

CREATE FUNCTION mapped_query() returns table (i integer) LANGUAGE PYTHON_MAP
{
    _conn.execute('CREATE TABLE pyapi19_integers(i INTEGER);')
    _conn.execute('INSERT INTO pyapi19_integers VALUES (0), (1), (2);')
    return(1)
};

SELECT * FROM mapped_query();
SELECT * FROM pyapi19_integers;

CREATE FUNCTION mapped_result_query() returns table (i integer) LANGUAGE PYTHON_MAP
{
    res = _conn.execute('SELECT * FROM pyapi19_integers;')
    return res['i']
};

SELECT * FROM mapped_result_query();

ROLLBACK;


START TRANSACTION;
# try a bigger set
CREATE FUNCTION pyapi19_create_table() returns table (i integer) LANGUAGE P
{
    return numpy.arange(100000)
};
CREATE FUNCTION pyapi19_load_table() returns table (i integer) LANGUAGE PYTHON_MAP
{
    res = _conn.execute('SELECT * FROM pyapi19_integers;')
    return res['i']
};

CREATE TABLE pyapi19_integers AS SELECT * FROM pyapi19_create_table() WITH DATA;
SELECT COUNT(i) FROM pyapi19_load_table();

ROLLBACK;


START TRANSACTION;
# test strings (these have a different interaction because of the varheap)
CREATE TABLE pyapi19_strings(s STRING);
INSERT INTO pyapi19_strings VALUES ('hello'), ('33'), ('hello world');

CREATE FUNCTION mapped_result_query() returns table (i STRING) LANGUAGE PYTHON_MAP
{
    res = _conn.execute('SELECT * FROM pyapi19_strings;')
    return res['s']
};

SELECT * FROM mapped_result_query();

ROLLBACK;

START TRANSACTION;
# try SQL types
CREATE TABLE pyapi19_dates(d DATE);
INSERT INTO pyapi19_dates VALUES (cast('2014-10-03' as DATE)), (cast('2000-03-24' as DATE)), ('2033-11-22');

CREATE FUNCTION mapped_result_query() returns table (d DATE) LANGUAGE PYTHON_MAP
{
    res = _conn.execute('SELECT * FROM pyapi19_dates;')
    return res
};

SELECT * FROM mapped_result_query();

ROLLBACK;

START TRANSACTION;
# try multiple columns of different types

CREATE FUNCTION pyapi19_create_table() returns table (i integer, j integer, k double, l float, m smallint, n bigint, o STRING, p DECIMAL) LANGUAGE P
{
    result = dict();
    result['i'] = numpy.arange(100000, 0, -1);
    result['j'] = numpy.arange(100000, 0 , -1);
    result['k'] = numpy.arange(100000);
    result['l'] = numpy.arange(100000, 0 , -1);
    result['m'] = numpy.tile(numpy.arange(100), 1000)
    result['n'] = numpy.arange(100000, 0 , -1);
    result['o'] = numpy.arange(100000);
    result['p'] = numpy.arange(100000);
    return result
};

CREATE FUNCTION pyapi19_load_table() returns table (i integer, j integer, k double, l float, m smallint, n bigint, o STRING, p DECIMAL) LANGUAGE PYTHON_MAP
{
    res = _conn.execute('SELECT * FROM pyapi19_integers;')
    return res
};

CREATE TABLE pyapi19_integers AS SELECT * FROM pyapi19_create_table() WITH DATA;
SELECT * FROM pyapi19_load_table() LIMIT 100;


ROLLBACK;
