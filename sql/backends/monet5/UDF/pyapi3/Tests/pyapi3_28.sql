
START TRANSACTION;

CREATE FUNCTION duplicate_strings() RETURNS TABLE(s STRING)  LANGUAGE PYTHON {
    return numpy.repeat('hello', 1000000)
};

CREATE FUNCTION test_duplicates(s STRING) RETURNS BOOLEAN LANGUAGE PYTHON {
    print(s)
    del s
    return True  
};

SELECT test_duplicates(s) FROM duplicate_strings();


ROLLBACK;
