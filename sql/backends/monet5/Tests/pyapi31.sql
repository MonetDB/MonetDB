
START TRANSACTION;

# generate data
CREATE FUNCTION pyapi31_gendata() RETURNS TABLE(i INTEGER) LANGUAGE PYTHON { return numpy.arange(100) };
CREATE TABLE input AS SELECT * FROM pyapi31_gendata() WITH DATA;

# parallel table-producing function
CREATE FUNCTION pyapi31_parallel(i INTEGER) 
RETURNS TABLE(a INTEGER, b INTEGER, c INTEGER) LANGUAGE PYTHON_MAP 
{ 
	return {'a': i, 'b': i * 2, 'c': i * 3};
};
# this function should be executed in parallel over the inputs
explain SELECT * FROM pyapi31_parallel((SELECT i FROM input));
SELECT * FROM pyapi31_parallel((SELECT i FROM input));


# while we're at it, parallel function with more than 3 inputs:
CREATE FUNCTION pyapi31_many_inputs(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER) RETURNS INTEGER LANGUAGE PYTHON_MAP { return a * b * c * d * e };

explain SELECT pyapi31_many_inputs(i, i, i, i, i) FROM input;
SELECT pyapi31_many_inputs(i, i, i, i, i) FROM input;

# but a function with 3 inputs does work 
CREATE FUNCTION pyapi31_three_inputs(a INTEGER, b INTEGER, c INTEGER) RETURNS INTEGER LANGUAGE PYTHON_MAP { return a * b * c };

explain SELECT pyapi31_three_inputs(i, i, i) FROM input;
SELECT pyapi31_three_inputs(i, i, i) FROM input;

ROLLBACK;
