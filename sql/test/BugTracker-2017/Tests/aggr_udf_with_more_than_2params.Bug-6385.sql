CREATE AGGREGATE aggr2(val INTEGER, val2 INTEGER)
RETURNS INTEGER
LANGUAGE PYTHON {
    unique = numpy.unique(aggr_group)
    x = numpy.zeros(shape=(unique.size))
    for i in range(0, unique.size):
        x[i] = numpy.sum(val[aggr_group==unique[i]])
    return(x)
};

CREATE AGGREGATE aggr3(val INTEGER, val2 INTEGER, val3 INTEGER)
RETURNS DOUBLE
LANGUAGE PYTHON {
    unique = numpy.unique(aggr_group)
    x = numpy.zeros(shape=(unique.size))
    for i in range(0, unique.size):
        x[i] = numpy.sum(val[aggr_group==unique[i]])
    return(x)
};

CREATE AGGREGATE aggr4(val INTEGER, val2 INTEGER, val3 INTEGER, val4 INTEGER)
RETURNS DOUBLE
LANGUAGE PYTHON {
    unique = numpy.unique(aggr_group)
    x = numpy.zeros(shape=(unique.size))
    for i in range(0, unique.size):
        x[i] = numpy.sum(val[aggr_group==unique[i]])
    return(x)
};

CREATE TABLE grouped_ints (value INTEGER, groupnr INTEGER, dbvalue double);
INSERT INTO  grouped_ints VALUES (1, 0, 0.11), (2, 1, 0.22), (3, 0, 0.33), (4,1, 0.44), (5,0, 0.55);

SELECT groupnr, aggr3(value, value, value) FROM grouped_ints GROUP BY groupnr;
SELECT groupnr, aggr4(value, value, value, value) FROM grouped_ints GROUP BY groupnr;

SELECT groupnr, aggr2(value) FROM grouped_ints GROUP BY groupnr;
SELECT groupnr, aggr2(value, value, value) FROM grouped_ints GROUP BY groupnr;

DROP TABLE grouped_ints;
DROP AGGREGATE aggr2;
DROP AGGREGATE aggr3;
DROP AGGREGATE aggr4;

