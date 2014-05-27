START TRANSACTION;

CREATE TABLE rval(groupcol integer,datacol integer);
INSERT INTO rval VALUES (1,42), (1,84), (2,42), (2,21);

CREATE AGGREGATE aggrmedian(val integer) RETURNS double LANGUAGE R {
	if (exists("aggr_group")) {
		return(aggregate(val, by=list(aggr_group), FUN=median)$x)
	} else {
		return(median(val))
	}

};

SELECT aggrmedian(datacol) FROM rval;
SELECT groupcol,aggrmedian(datacol) FROM rval GROUP BY groupcol;


DROP AGGREGATE aggrmedian;
DROP TABLE rval;

ROLLBACK;
