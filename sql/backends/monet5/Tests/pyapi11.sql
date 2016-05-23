# Test nested python calls
# Basically map/reduce type stuff where we make one function execute in parallel and then pass the data to a blocking operation to do something that can't be parallelized 
START TRANSACTION;


CREATE FUNCTION pyapi11_random_table_nulls(entries integer) returns table (i integer, j integer)
language P
{
    numpy.random.seed(123)
    res = {'i': numpy.random.randint(0, 100, entries), 'j': numpy.random.randint(0, 100, entries)}
    return {'i': numpy.ma.masked_array(res['i'], mask=res['i'] < 50), 'j': numpy.ma.masked_array(res['j'], mask=res['j'] < 50)}
};


CREATE FUNCTION pyapi11_mult(i integer,j integer) returns integer
language PYTHON_MAP
{
    return(i*j)
};

CREATE FUNCTION pyapi11_mean(i integer) returns double
language PYTHON
{
    return(numpy.mean(i))
};

# Transfer the output from one python function to another
SELECT pyapi11_mean(pyapi11_mult(i,j)) FROM pyapi11_random_table_nulls(5000);

DROP FUNCTION pyapi11_random_table_nulls;
DROP FUNCTION pyapi11_mean;
DROP FUNCTION pyapi11_mult;

ROLLBACK;
