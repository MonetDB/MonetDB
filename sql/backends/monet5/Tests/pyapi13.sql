# Test various whitespace configurations
# Including tests for multiline strings (strings starting with """)

START TRANSACTION;

CREATE FUNCTION pyapi13_random_table_nulls(entries integer) returns table (i integer, j integer)
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

#"normal" indentation
CREATE FUNCTION pyapi13_mult(i integer,j integer) returns integer
language PYTHON_MAP
{
    return(i*j)
};
SELECT COUNT(pyapi13_mult(i,j)) FROM pyapi13_random_table_nulls(5000);
DROP FUNCTION pyapi13_mult;

#weird indentation
CREATE FUNCTION pyapi13_mult(i integer,j integer) returns integer
language PYTHON_MAP { return(i*j)
};
SELECT COUNT(pyapi13_mult(i,j)) FROM pyapi13_random_table_nulls(5000);
DROP FUNCTION pyapi13_mult;

#no new line
CREATE FUNCTION pyapi13_mult(i integer,j integer) returns integer language PYTHON_MAP { return(i*j) };
SELECT COUNT(pyapi13_mult(i,j)) FROM pyapi13_random_table_nulls(5000);
DROP FUNCTION pyapi13_mult;

#\n in string
CREATE FUNCTION pyapi13_mult(i integer,j integer) returns integer 
language PYTHON_MAP 
{ 
x = "test\n\ntesttest\n"
print(x)
return(i*j) 
};
SELECT COUNT(pyapi13_mult(i,j)) FROM pyapi13_random_table_nulls(5000);
DROP FUNCTION pyapi13_mult;

#Multiline statements
CREATE FUNCTION pyapi13_mult(i integer,j integer) returns integer 
language PYTHON_MAP 
{ 
x = """test123
testtest
""test2""
hello world
"""
print(x)
if len(x) > 10:
	return(i*j) 
else:
	return(i+j)
};
SELECT COUNT(pyapi13_mult(i,j)) FROM pyapi13_random_table_nulls(5000);
DROP FUNCTION pyapi13_mult;

#inconsistent indentation (mix spaces and tabs, weird indentation)
CREATE FUNCTION pyapi13_mult(i integer,j integer) returns integer 
language PYTHON_MAP 
{ 
	x = 5
    y = 4
    z = x + y
	if z > x:
     print("y is not negative!")
    else:
     print("y is negative!")
    if x + y == z:
    	print("Addition in python is not inconsistent!")
    	return(i + j)
    else:
    								return(i*j)
};
SELECT COUNT(pyapi13_mult(i,j)) FROM pyapi13_random_table_nulls(5000);
DROP FUNCTION pyapi13_mult;

DROP FUNCTION pyapi13_random_table_nulls;
ROLLBACK;
