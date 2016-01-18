# Test nested python calls
# Basically map/reduce type stuff where we make one function execute in parallel and then pass the data to a blocking operation to do something that can't be parallelized 
START TRANSACTION;


CREATE FUNCTION pyapi11_random_table_nulls(entries integer) returns table (i integer, j integer)
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
