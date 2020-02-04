
START TRANSACTION;


CREATE FUNCTION pyapi32_gentbl() RETURNS TABLE(i TINYINT) LANGUAGE PYTHON {
	return { 'i': numpy.arange(100) }
};

CREATE FUNCTION pyapi32_function(i INTEGER) RETURNS INTEGER LANGUAGE PYTHON {
	return i * 2;	
};

CREATE FUNCTION pyapi32_function_mapped(i INTEGER) RETURNS INTEGER LANGUAGE PYTHON_MAP {
	return i * 2;	
};

CREATE TABLE integers AS SELECT * FROM pyapi32_gentbl() WITH DATA;

SELECT pyapi32_function(i) FROM integers;
SELECT pyapi32_function(i) FROM integers WHERE i > 50;

SELECT pyapi32_function_mapped(i) FROM integers;
SELECT pyapi32_function_mapped(i) FROM integers WHERE i > 50;

ROLLBACK;
