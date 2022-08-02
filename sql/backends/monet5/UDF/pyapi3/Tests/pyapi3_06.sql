# Test aggregations with the hidden variable 'aggr_group'. 
# Note that if we do an aggregation without GROUP BY the variable 'aggr_group' does not exist.
# To handle this case, we have to check if aggr_group is in the locals.
START TRANSACTION;

CREATE TABLE rval(groupcol integer,datacol integer);
INSERT INTO rval VALUES (1,42), (1,84), (2,42), (2,21);

CREATE AGGREGATE aggrmedian(val integer) RETURNS integer LANGUAGE P {
	if 'aggr_group' in locals():
		unique = numpy.unique(aggr_group)
		x = numpy.zeros(shape=(unique.size))
		for i in range(0,unique.size):
			x[i] = numpy.median(val[numpy.where(aggr_group==unique[i])])
		return(x)
	else:
		return(numpy.median(val))
};

SELECT aggrmedian(datacol) FROM rval;
SELECT groupcol,aggrmedian(datacol) FROM rval GROUP BY groupcol;


DROP AGGREGATE aggrmedian;
DROP TABLE rval;

ROLLBACK;

# Aggregation on multiple columns
START TRANSACTION;

CREATE TABLE rval(groupcol integer, secondgroupcol integer, datacol integer);
INSERT INTO rval VALUES (0, 0, 1), (0, 0, 2), (0, 1, 3), (0, 1, 4), (1, 0, 5), (1, 0, 6), (1, 1, 7), (1, 1, 8);

CREATE AGGREGATE aggrsum(val integer) RETURNS integer LANGUAGE P {
	unique = numpy.unique(aggr_group)
	x = numpy.zeros(shape=(unique.size))
	for i in range(0, unique.size):
		x[i] = numpy.sum(val[numpy.where(aggr_group==unique[i])])
	return x
};

SELECT groupcol,secondgroupcol,aggrsum(datacol) FROM rval GROUP BY groupcol,secondgroupcol;


DROP AGGREGATE aggrsum;
DROP TABLE rval;

ROLLBACK;
