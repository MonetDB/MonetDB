# Basic multiprocessing test using PYTHON_MAP
# We also test if error codes work correctly when the child process fails
# Because the error occurs in a child process separate from the main process when multiprocessing is enabled we have to explicity send the error message back to the main process
# This could go wrong, so we test it 

START TRANSACTION;

CREATE FUNCTION pyapi10_random_table(entries integer) returns table (i integer, j integer)
language P
{
    import random
    random.seed(123)
    results = [numpy.zeros(entries), numpy.zeros(entries)]
    for i in range(0,entries):
        results[0][i] = random.randint(0,100)
        results[1][i] = random.randint(0,100)
    return(results)
};

CREATE FUNCTION pyapi10_random_table_nulls(entries integer) returns table (i integer, j integer)
language P
{
    import random
    random.seed(123)
    results = [numpy.ma.masked_array(numpy.zeros(entries), 0), numpy.ma.masked_array(numpy.zeros(entries), 0)]
    for i in range(0,entries):
        for j in range(0,2):
            results[j][i] = random.randint(0,100)
            if results[j][i] < 50:
                results[j].mask[i] = True
    return(results)
};


CREATE FUNCTION pyapi10_mult(i integer,j integer) returns integer
language PYTHON_MAP
{
    return(i*j)
};

# This function is incorrect on purpose
CREATE FUNCTION pyapi10_error(i integer,j integer) returns integer
language PYTHON_MAP
{
    return(i*j 
};

# Test regular multiprocessing
SELECT AVG(pyapi10_mult(i,j)) FROM pyapi10_random_table(5000);
# Test multiprocessing with NULLs
SELECT AVG(pyapi10_mult(i,j)) FROM pyapi10_random_table_nulls(5000);

DROP FUNCTION pyapi10_random_table;
DROP FUNCTION pyapi10_mult;

# Test errors during multiprocessing
SELECT AVG(pyapi10_error(i,j)) FROM pyapi10_random_table_nulls(5000);

ROLLBACK;

START TRANSACTION;
CREATE FUNCTION pyapi10_random_table_nulls(entries integer) returns table (i integer, j integer)
language P
{
    import random
    random.seed(123)
    results = [numpy.ma.masked_array(numpy.zeros(entries), 0), numpy.ma.masked_array(numpy.zeros(entries), 0)]
    for i in range(0,entries):
        for j in range(0,2):
            results[j][i] = random.randint(0,100)
            if results[j][i] < 50:
                results[j].mask[i] = True
    return(results)
};

# This function is incorrect on purpose
CREATE FUNCTION pyapi10_indentation_error(i integer,j integer) returns integer
language PYTHON_MAP
{   a = 1;
    b = 2
    c = 3
    d = 4
    e = 5
    f = 6
    g = 7
     h = 8
    i = 9
    j = 10
    k = 11
    l = 12
    m = 13
    n = 14
    return a+b+c+d+e+f+g+h
};

# Test errors during multiprocessing
SELECT AVG(pyapi10_indentation_error(i,j)) FROM pyapi10_random_table_nulls(5000);

ROLLBACK;

# This function is incorrect on purpose
CREATE FUNCTION pyapi10_indentation_error(i integer,j integer) returns integer
language PYTHON_MAP
{   a = 1;
    b = 2
    c = 3
    d = 4
    e = 5
    f = 6
    g = 7
     h = 8
    i = 9
    j = 10
    k = 11
    l = 12
    m = 13
    n = 14
    return a+b+c+d+e+f+g+h
};


# Let's try a runtime exception
START TRANSACTION;

CREATE FUNCTION pyapi10_random_table_nulls(entries integer) returns table (i integer, j integer)
language P
{
    import random
    random.seed(123)
    results = [numpy.ma.masked_array(numpy.zeros(entries), 0), numpy.ma.masked_array(numpy.zeros(entries), 0)]
    for i in range(0,entries):
        for j in range(0,2):
            results[j][i] = random.randint(0,100)
            if results[j][i] < 50:
                results[j].mask[i] = True
    return(results)
};

# This function is incorrect on purpose
CREATE FUNCTION pyapi10_runtime_exception(i integer,j integer) returns integer
language PYTHON_MAP
{   
    return_val = i * j;
    myint = 5
    mydivision = 0
    # comments
    hello = myint / mydivision
    # comments
    return return_val
};

# Test errors during multiprocessing
SELECT AVG(pyapi10_runtime_exception(i,j)) FROM pyapi10_random_table_nulls(5000);

ROLLBACK;
