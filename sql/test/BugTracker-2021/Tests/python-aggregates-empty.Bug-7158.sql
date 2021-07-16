START TRANSACTION;

CREATE TABLE test (x INTEGER);

CREATE AGGREGATE python_aggregate(val INTEGER)
RETURNS INTEGER
LANGUAGE PYTHON {
    try:
        unique = numpy.unique(aggr_group)
        x = numpy.zeros(shape=(unique.size))
        for i in range(0, unique.size):
            x[i] = numpy.sum(val[aggr_group==unique[i]])
    except NameError:
        # aggr_group does not exist. no groups, aggregate on all data
        x = numpy.sum(val)
    return(x)
};

SELECT python_aggregate(x) FROM test;

SELECT python_aggregate(x) FROM test group by x;

CREATE FUNCTION myfunc(val INTEGER)
RETURNS INTEGER 
LANGUAGE PYTHON {
    return val
};

SELECT myfunc(x) FROM test;

ROLLBACK;
