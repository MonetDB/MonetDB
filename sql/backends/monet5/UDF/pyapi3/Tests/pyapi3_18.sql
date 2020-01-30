# This test script tests the correct interaction of the SQL types DATE, TIME, TIMESTAMP and DECIMAL with MonetDB/Python
# These values have special conversions so they make sense to the user (as the internal type of these are encoded values)

START TRANSACTION;
# Test input of SQL types in MonetDB/Python functions
CREATE TABLE date_table(d DATE); # DATE is converted to str
INSERT INTO date_table VALUES (cast('2000-10-10' AS DATE));
CREATE FUNCTION pyapi_date(d DATE) RETURNS STRING LANGUAGE PYTHON { return d; };
SELECT pyapi_date(d) FROM date_table;

CREATE TABLE time_table(d TIME); # TIME is converted to str
INSERT INTO time_table VALUES (cast('12:00:00' AS TIME));
CREATE FUNCTION pyapi_time(d TIME) RETURNS STRING LANGUAGE PYTHON { return d; };
SELECT pyapi_time(d) FROM time_table;

CREATE TABLE timestamp_table(d TIMESTAMP); # TIMESTAMP is converted to str
INSERT INTO timestamp_table VALUES (cast('2000-1-1 12:00:00' AS TIMESTAMP));
CREATE FUNCTION pyapi_timestamp(d TIMESTAMP) RETURNS STRING LANGUAGE PYTHON { return d; };
SELECT pyapi_timestamp(d) FROM timestamp_table;

CREATE TABLE decimal_table(d DECIMAL(10, 3)); # DECIMAL is converted to dbl
INSERT INTO decimal_table VALUES (123.4567);
CREATE FUNCTION pyapi_decimal(d DECIMAL) RETURNS DOUBLE LANGUAGE PYTHON { return d; };
SELECT pyapi_decimal(d) FROM decimal_table;

# Test returning SQL types from MonetDB/Python functions
CREATE FUNCTION pyapi_ret_date() RETURNS TABLE(d DATE) 
LANGUAGE PYTHON 
{ 
    result = dict()
    result['d'] = '2000-10-10'
    return result
};
SELECT * FROM pyapi_ret_date();

CREATE FUNCTION pyapi_ret_time() RETURNS TABLE(d TIME) 
LANGUAGE PYTHON 
{ 
    result = dict()
    result['d'] = '12:00:00'
    return result
};
SELECT * FROM pyapi_ret_time();

CREATE FUNCTION pyapi_ret_timestamp() RETURNS TABLE(d TIMESTAMP) 
LANGUAGE PYTHON 
{ 
    result = dict()
    result['d'] = '2000-1-1 12:00:00'
    return result
};
SELECT * FROM pyapi_ret_timestamp();

CREATE FUNCTION pyapi_ret_decimal() RETURNS TABLE(d DECIMAL) 
LANGUAGE PYTHON 
{ 
    result = dict()
    result['d'] = 100.33
    return result
};
SELECT * FROM pyapi_ret_decimal();

# Now test bulk input/output

DROP FUNCTION pyapi_ret_date;
DROP FUNCTION pyapi_ret_time;
DROP FUNCTION pyapi_ret_timestamp;
DROP FUNCTION pyapi_ret_decimal;

CREATE FUNCTION pyapi_ret_decimal() RETURNS TABLE(d DECIMAL) 
LANGUAGE PYTHON 
{ 
    return numpy.arange(100001) # return 100k decimal values
};

CREATE FUNCTION pyapi_inp_decimal(d DECIMAL) RETURNS DOUBLE
LANGUAGE PYTHON 
{ 
    return numpy.mean(d) # average 100k decimal values
};

SELECT pyapi_inp_decimal(d) FROM pyapi_ret_decimal();

# test unsupported type
CREATE TABLE uuid_tbl(d UUID);
INSERT INTO uuid_tbl VALUES ('54771a16-b4ad-4f1a-a9b7-4d8e8ca6fb7c');
CREATE FUNCTION pyapi_interval(d UUID) RETURNS STRING LANGUAGE PYTHON { return d; };
SELECT pyapi_interval(d) FROM uuid_tbl;
ROLLBACK;
