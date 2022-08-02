CREATE AGGREGATE python_aggregate(val INTEGER)
RETURNS INTEGER
LANGUAGE PYTHON {
    try:
        unique = numpy.unique(aggr_group)
        x = numpy.zeros(shape=(unique.size))
        for i in range(0, unique.size):
            x[i] = numpy.sum(val[aggr_group==unique[i]])
    except NameError:
        # aggr_group doesn't exist. no groups, aggregate on all data
        x = numpy.sum(val)
    return(x)
};

CREATE TABLE grouped_ints(value INTEGER, groupnr INTEGER);
INSERT INTO grouped_ints VALUES (1, 0), (2, 1), (3, 0), (4, 1), (5, 0);
SELECT groupnr, python_aggregate(value) FROM grouped_ints GROUP BY groupnr;
SELECT value, python_aggregate(groupnr) FROM grouped_ints GROUP BY value;
SELECT python_aggregate(groupnr) FROM grouped_ints;
SELECT python_aggregate(value) FROM grouped_ints;
DROP TABLE grouped_ints;

CREATE TABLE grouped_ints(value INTEGER, groupnr INTEGER, groupnr2 INTEGER);
INSERT INTO grouped_ints VALUES (1, 0, 0), (2, 0, 0), (3, 0, 1), (4, 0, 1), (5, 1, 0), (6, 1, 0), (7, 1, 1), (8, 1, 1);
SELECT groupnr, groupnr2, python_aggregate(value) FROM grouped_ints GROUP BY groupnr, groupnr2;
SELECT python_aggregate(groupnr) FROM grouped_ints;
SELECT python_aggregate(groupnr2) FROM grouped_ints;
SELECT python_aggregate(value) FROM grouped_ints;
DROP TABLE grouped_ints;

DROP AGGREGATE python_aggregate(INTEGER);

