-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

create aggregate stddev_samp(val TINYINT) returns DOUBLE
	external name "aggr"."stdev";
GRANT EXECUTE ON AGGREGATE stddev_samp(TINYINT) TO PUBLIC;
create aggregate stddev_samp(val SMALLINT) returns DOUBLE
	external name "aggr"."stdev";
GRANT EXECUTE ON AGGREGATE stddev_samp(SMALLINT) TO PUBLIC;
create aggregate stddev_samp(val INTEGER) returns DOUBLE
	external name "aggr"."stdev";
GRANT EXECUTE ON AGGREGATE stddev_samp(INTEGER) TO PUBLIC;
create aggregate stddev_samp(val BIGINT) returns DOUBLE
	external name "aggr"."stdev";
GRANT EXECUTE ON AGGREGATE stddev_samp(BIGINT) TO PUBLIC;
create aggregate stddev_samp(val REAL) returns DOUBLE
	external name "aggr"."stdev";
GRANT EXECUTE ON AGGREGATE stddev_samp(REAL) TO PUBLIC;
create aggregate stddev_samp(val DOUBLE) returns DOUBLE
	external name "aggr"."stdev";
GRANT EXECUTE ON AGGREGATE stddev_samp(DOUBLE) TO PUBLIC;

create aggregate stddev_samp(val INTERVAL SECOND) returns DOUBLE
	external name "aggr"."stdev";
GRANT EXECUTE ON AGGREGATE stddev_samp(INTERVAL SECOND) TO PUBLIC;
create aggregate stddev_samp(val INTERVAL MONTH) returns DOUBLE
	external name "aggr"."stdev";
GRANT EXECUTE ON AGGREGATE stddev_samp(INTERVAL MONTH) TO PUBLIC;

create window stddev_samp(val TINYINT) returns DOUBLE
	external name "sql"."stdev";
GRANT EXECUTE ON WINDOW stddev_samp(TINYINT) TO PUBLIC;
create window stddev_samp(val SMALLINT) returns DOUBLE
	external name "sql"."stdev";
GRANT EXECUTE ON WINDOW stddev_samp(SMALLINT) TO PUBLIC;
create window stddev_samp(val INTEGER) returns DOUBLE
	external name "sql"."stdev";
GRANT EXECUTE ON WINDOW stddev_samp(INTEGER) TO PUBLIC;
create window stddev_samp(val BIGINT) returns DOUBLE
	external name "sql"."stdev";
GRANT EXECUTE ON WINDOW stddev_samp(BIGINT) TO PUBLIC;
create window stddev_samp(val REAL) returns DOUBLE
	external name "sql"."stdev";
GRANT EXECUTE ON WINDOW stddev_samp(REAL) TO PUBLIC;
create window stddev_samp(val DOUBLE) returns DOUBLE
	external name "sql"."stdev";
GRANT EXECUTE ON WINDOW stddev_samp(DOUBLE) TO PUBLIC;

create window stddev_samp(val INTERVAL SECOND) returns DOUBLE
	external name "sql"."stdev";
GRANT EXECUTE ON WINDOW stddev_samp(INTERVAL SECOND) TO PUBLIC;
create window stddev_samp(val INTERVAL MONTH) returns DOUBLE
	external name "sql"."stdev";
GRANT EXECUTE ON WINDOW stddev_samp(INTERVAL MONTH) TO PUBLIC;


create aggregate stddev_pop(val TINYINT) returns DOUBLE
	external name "aggr"."stdevp";
GRANT EXECUTE ON AGGREGATE stddev_pop(TINYINT) TO PUBLIC;
create aggregate stddev_pop(val SMALLINT) returns DOUBLE
	external name "aggr"."stdevp";
GRANT EXECUTE ON AGGREGATE stddev_pop(SMALLINT) TO PUBLIC;
create aggregate stddev_pop(val INTEGER) returns DOUBLE
	external name "aggr"."stdevp";
GRANT EXECUTE ON AGGREGATE stddev_pop(INTEGER) TO PUBLIC;
create aggregate stddev_pop(val BIGINT) returns DOUBLE
	external name "aggr"."stdevp";
GRANT EXECUTE ON AGGREGATE stddev_pop(BIGINT) TO PUBLIC;
create aggregate stddev_pop(val REAL) returns DOUBLE
	external name "aggr"."stdevp";
GRANT EXECUTE ON AGGREGATE stddev_pop(REAL) TO PUBLIC;
create aggregate stddev_pop(val DOUBLE) returns DOUBLE
	external name "aggr"."stdevp";
GRANT EXECUTE ON AGGREGATE stddev_pop(DOUBLE) TO PUBLIC;

create aggregate stddev_pop(val INTERVAL SECOND) returns DOUBLE
	external name "aggr"."stdevp";
GRANT EXECUTE ON AGGREGATE stddev_pop(INTERVAL SECOND) TO PUBLIC;
create aggregate stddev_pop(val INTERVAL MONTH) returns DOUBLE
	external name "aggr"."stdevp";
GRANT EXECUTE ON AGGREGATE stddev_pop(INTERVAL MONTH) TO PUBLIC;

create window stddev_pop(val TINYINT) returns DOUBLE
	external name "sql"."stdevp";
GRANT EXECUTE ON WINDOW stddev_pop(TINYINT) TO PUBLIC;
create window stddev_pop(val SMALLINT) returns DOUBLE
	external name "sql"."stdevp";
GRANT EXECUTE ON WINDOW stddev_pop(SMALLINT) TO PUBLIC;
create window stddev_pop(val INTEGER) returns DOUBLE
	external name "sql"."stdevp";
GRANT EXECUTE ON WINDOW stddev_pop(INTEGER) TO PUBLIC;
create window stddev_pop(val BIGINT) returns DOUBLE
	external name "sql"."stdevp";
GRANT EXECUTE ON WINDOW stddev_pop(BIGINT) TO PUBLIC;
create window stddev_pop(val REAL) returns DOUBLE
	external name "sql"."stdevp";
GRANT EXECUTE ON WINDOW stddev_pop(REAL) TO PUBLIC;
create window stddev_pop(val DOUBLE) returns DOUBLE
	external name "sql"."stdevp";
GRANT EXECUTE ON WINDOW stddev_pop(DOUBLE) TO PUBLIC;

create window stddev_pop(val INTERVAL SECOND) returns DOUBLE
	external name "sql"."stdevp";
GRANT EXECUTE ON WINDOW stddev_pop(INTERVAL SECOND) TO PUBLIC;
create window stddev_pop(val INTERVAL MONTH) returns DOUBLE
	external name "sql"."stdevp";
GRANT EXECUTE ON WINDOW stddev_pop(INTERVAL MONTH) TO PUBLIC;


create aggregate var_samp(val TINYINT) returns DOUBLE
	external name "aggr"."variance";
GRANT EXECUTE ON AGGREGATE var_samp(TINYINT) TO PUBLIC;
create aggregate var_samp(val SMALLINT) returns DOUBLE
	external name "aggr"."variance";
GRANT EXECUTE ON AGGREGATE var_samp(SMALLINT) TO PUBLIC;
create aggregate var_samp(val INTEGER) returns DOUBLE
	external name "aggr"."variance";
GRANT EXECUTE ON AGGREGATE var_samp(INTEGER) TO PUBLIC;
create aggregate var_samp(val BIGINT) returns DOUBLE
	external name "aggr"."variance";
GRANT EXECUTE ON AGGREGATE var_samp(BIGINT) TO PUBLIC;
create aggregate var_samp(val REAL) returns DOUBLE
	external name "aggr"."variance";
GRANT EXECUTE ON AGGREGATE var_samp(REAL) TO PUBLIC;
create aggregate var_samp(val DOUBLE) returns DOUBLE
	external name "aggr"."variance";
GRANT EXECUTE ON AGGREGATE var_samp(DOUBLE) TO PUBLIC;

create aggregate var_samp(val INTERVAL SECOND) returns DOUBLE
	external name "aggr"."variance";
GRANT EXECUTE ON AGGREGATE var_samp(INTERVAL SECOND) TO PUBLIC;
create aggregate var_samp(val INTERVAL MONTH) returns DOUBLE
	external name "aggr"."variance";
GRANT EXECUTE ON AGGREGATE var_samp(INTERVAL MONTH) TO PUBLIC;

create window var_samp(val TINYINT) returns DOUBLE
	external name "sql"."variance";
GRANT EXECUTE ON WINDOW var_samp(TINYINT) TO PUBLIC;
create window var_samp(val SMALLINT) returns DOUBLE
	external name "sql"."variance";
GRANT EXECUTE ON WINDOW var_samp(SMALLINT) TO PUBLIC;
create window var_samp(val INTEGER) returns DOUBLE
	external name "sql"."variance";
GRANT EXECUTE ON WINDOW var_samp(INTEGER) TO PUBLIC;
create window var_samp(val BIGINT) returns DOUBLE
	external name "sql"."variance";
GRANT EXECUTE ON WINDOW var_samp(BIGINT) TO PUBLIC;
create window var_samp(val REAL) returns DOUBLE
	external name "sql"."variance";
GRANT EXECUTE ON WINDOW var_samp(REAL) TO PUBLIC;
create window var_samp(val DOUBLE) returns DOUBLE
	external name "sql"."variance";
GRANT EXECUTE ON WINDOW var_samp(DOUBLE) TO PUBLIC;

create window var_samp(val INTERVAL SECOND) returns DOUBLE
	external name "sql"."variance";
GRANT EXECUTE ON WINDOW var_samp(INTERVAL SECOND) TO PUBLIC;
create window var_samp(val INTERVAL MONTH) returns DOUBLE
	external name "sql"."variance";
GRANT EXECUTE ON WINDOW var_samp(INTERVAL MONTH) TO PUBLIC;


create aggregate var_pop(val TINYINT) returns DOUBLE
	external name "aggr"."variancep";
GRANT EXECUTE ON AGGREGATE var_pop(TINYINT) TO PUBLIC;
create aggregate var_pop(val SMALLINT) returns DOUBLE
	external name "aggr"."variancep";
GRANT EXECUTE ON AGGREGATE var_pop(SMALLINT) TO PUBLIC;
create aggregate var_pop(val INTEGER) returns DOUBLE
	external name "aggr"."variancep";
GRANT EXECUTE ON AGGREGATE var_pop(INTEGER) TO PUBLIC;
create aggregate var_pop(val BIGINT) returns DOUBLE
	external name "aggr"."variancep";
GRANT EXECUTE ON AGGREGATE var_pop(BIGINT) TO PUBLIC;
create aggregate var_pop(val REAL) returns DOUBLE
	external name "aggr"."variancep";
GRANT EXECUTE ON AGGREGATE var_pop(REAL) TO PUBLIC;
create aggregate var_pop(val DOUBLE) returns DOUBLE
	external name "aggr"."variancep";
GRANT EXECUTE ON AGGREGATE var_pop(DOUBLE) TO PUBLIC;
create aggregate var_pop(val INTERVAL SECOND) returns DOUBLE
	external name "aggr"."variancep";
GRANT EXECUTE ON AGGREGATE var_pop(INTERVAL SECOND) TO PUBLIC;
create aggregate var_pop(val INTERVAL MONTH) returns DOUBLE
	external name "aggr"."variancep";
GRANT EXECUTE ON AGGREGATE var_pop(INTERVAL MONTH) TO PUBLIC;

create window var_pop(val TINYINT) returns DOUBLE
	external name "sql"."variancep";
GRANT EXECUTE ON WINDOW var_pop(TINYINT) TO PUBLIC;
create window var_pop(val SMALLINT) returns DOUBLE
	external name "sql"."variancep";
GRANT EXECUTE ON WINDOW var_pop(SMALLINT) TO PUBLIC;
create window var_pop(val INTEGER) returns DOUBLE
	external name "sql"."variancep";
GRANT EXECUTE ON WINDOW var_pop(INTEGER) TO PUBLIC;
create window var_pop(val BIGINT) returns DOUBLE
	external name "sql"."variancep";
GRANT EXECUTE ON WINDOW var_pop(BIGINT) TO PUBLIC;
create window var_pop(val REAL) returns DOUBLE
	external name "sql"."variancep";
GRANT EXECUTE ON WINDOW var_pop(REAL) TO PUBLIC;
create window var_pop(val DOUBLE) returns DOUBLE
	external name "sql"."variancep";
GRANT EXECUTE ON WINDOW var_pop(DOUBLE) TO PUBLIC;

create window var_pop(val INTERVAL SECOND) returns DOUBLE
	external name "sql"."variancep";
GRANT EXECUTE ON WINDOW var_pop(INTERVAL SECOND) TO PUBLIC;
create window var_pop(val INTERVAL MONTH) returns DOUBLE
	external name "sql"."variancep";
GRANT EXECUTE ON WINDOW var_pop(INTERVAL MONTH) TO PUBLIC;


create aggregate median(val TINYINT) returns TINYINT
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(TINYINT) TO PUBLIC;
create aggregate median(val SMALLINT) returns SMALLINT
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(SMALLINT) TO PUBLIC;
create aggregate median(val INTEGER) returns INTEGER
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(INTEGER) TO PUBLIC;
create aggregate median(val BIGINT) returns BIGINT
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(BIGINT) TO PUBLIC;
create aggregate median(val DECIMAL) returns DECIMAL
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(DECIMAL) TO PUBLIC;
create aggregate median(val REAL) returns REAL
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(REAL) TO PUBLIC;
create aggregate median(val DOUBLE) returns DOUBLE
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(DOUBLE) TO PUBLIC;

create aggregate median(val DATE) returns DATE
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(DATE) TO PUBLIC;
create aggregate median(val TIME) returns TIME
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(TIME) TO PUBLIC;
create aggregate median(val TIMESTAMP) returns TIMESTAMP
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(TIMESTAMP) TO PUBLIC;
create aggregate median(val INTERVAL SECOND) returns INTERVAL SECOND
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(INTERVAL SECOND) TO PUBLIC;
create aggregate median(val INTERVAL MONTH) returns INTERVAL MONTH
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(INTERVAL MONTH) TO PUBLIC;

create window median(val TINYINT) returns TINYINT
	external name "sql"."median";
GRANT EXECUTE ON WINDOW median(TINYINT) TO PUBLIC;
create window median(val SMALLINT) returns SMALLINT
	external name "sql"."median";
GRANT EXECUTE ON WINDOW median(SMALLINT) TO PUBLIC;
create window median(val INTEGER) returns INTEGER
	external name "sql"."median";
GRANT EXECUTE ON WINDOW median(INTEGER) TO PUBLIC;
create window median(val BIGINT) returns BIGINT
	external name "sql"."median";
GRANT EXECUTE ON WINDOW median(BIGINT) TO PUBLIC;
create window median(val DECIMAL) returns DECIMAL
	external name "sql"."median";
GRANT EXECUTE ON WINDOW median(DECIMAL) TO PUBLIC;
create window median(val REAL) returns REAL
	external name "sql"."median";
GRANT EXECUTE ON WINDOW median(REAL) TO PUBLIC;
create window median(val DOUBLE) returns DOUBLE
	external name "sql"."median";
GRANT EXECUTE ON WINDOW median(DOUBLE) TO PUBLIC;

create window median(val DATE) returns DATE
	external name "sql"."median";
GRANT EXECUTE ON WINDOW median(DATE) TO PUBLIC;
create window median(val TIME) returns TIME
	external name "sql"."median";
GRANT EXECUTE ON WINDOW median(TIME) TO PUBLIC;
create window median(val TIMESTAMP) returns TIMESTAMP
	external name "sql"."median";
GRANT EXECUTE ON WINDOW median(TIMESTAMP) TO PUBLIC;
create window median(val INTERVAL SECOND) returns INTERVAL SECOND
	external name "sql"."median";
GRANT EXECUTE ON WINDOW median(INTERVAL SECOND) TO PUBLIC;
create window median(val INTERVAL MONTH) returns INTERVAL MONTH
	external name "sql"."median";
GRANT EXECUTE ON WINDOW median(INTERVAL MONTH) TO PUBLIC;


create aggregate quantile(val TINYINT, q DOUBLE) returns TINYINT
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(TINYINT, DOUBLE) TO PUBLIC;
create aggregate quantile(val SMALLINT, q DOUBLE) returns SMALLINT
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(SMALLINT, DOUBLE) TO PUBLIC;
create aggregate quantile(val INTEGER, q DOUBLE) returns INTEGER
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(INTEGER, DOUBLE) TO PUBLIC;
create aggregate quantile(val BIGINT, q DOUBLE) returns BIGINT
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(BIGINT, DOUBLE) TO PUBLIC;
create aggregate quantile(val DECIMAL, q DOUBLE) returns DECIMAL
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(DECIMAL, DOUBLE) TO PUBLIC;
create aggregate quantile(val REAL, q DOUBLE) returns REAL
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(REAL, DOUBLE) TO PUBLIC;
create aggregate quantile(val DOUBLE, q DOUBLE) returns DOUBLE
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(DOUBLE, DOUBLE) TO PUBLIC;

create aggregate quantile(val DATE, q DOUBLE) returns DATE
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(DATE, DOUBLE) TO PUBLIC;
create aggregate quantile(val TIME, q DOUBLE) returns TIME
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(TIME, DOUBLE) TO PUBLIC;
create aggregate quantile(val TIMESTAMP, q DOUBLE) returns TIMESTAMP
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(TIMESTAMP, DOUBLE) TO PUBLIC;
create aggregate quantile(val INTERVAL SECOND, q DOUBLE) returns INTERVAL SECOND
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(INTERVAL SECOND, DOUBLE) TO PUBLIC;
create aggregate quantile(val INTERVAL MONTH, q DOUBLE) returns INTERVAL MONTH
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(INTERVAL MONTH, DOUBLE) TO PUBLIC;

create window quantile(val TINYINT, q REAL) returns TINYINT
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(TINYINT, REAL) TO PUBLIC;
create window quantile(val SMALLINT, q REAL) returns SMALLINT
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(SMALLINT, REAL) TO PUBLIC;
create window quantile(val INTEGER, q REAL) returns INTEGER
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(INTEGER, REAL) TO PUBLIC;
create window quantile(val BIGINT, q REAL) returns BIGINT
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(BIGINT, REAL) TO PUBLIC;
create window quantile(val DECIMAL, q REAL) returns DECIMAL
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(DECIMAL, REAL) TO PUBLIC;
create window quantile(val REAL, q REAL) returns REAL
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(REAL, REAL) TO PUBLIC;
create window quantile(val DOUBLE, q REAL) returns DOUBLE
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(DOUBLE, REAL) TO PUBLIC;

create window quantile(val DATE, q REAL) returns DATE
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(DATE, REAL) TO PUBLIC;
create window quantile(val TIME, q REAL) returns TIME
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(TIME, REAL) TO PUBLIC;
create window quantile(val TIMESTAMP, q REAL) returns TIMESTAMP
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(TIMESTAMP, REAL) TO PUBLIC;
create window quantile(val INTERVAL SECOND, q REAL) returns INTERVAL SECOND
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(INTERVAL SECOND, REAL) TO PUBLIC;
create window quantile(val INTERVAL MONTH, q REAL) returns INTERVAL MONTH
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(INTERVAL MONTH, REAL) TO PUBLIC;

create window quantile(val TINYINT, q DOUBLE) returns TINYINT
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(TINYINT, DOUBLE) TO PUBLIC;
create window quantile(val SMALLINT, q DOUBLE) returns SMALLINT
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(SMALLINT, DOUBLE) TO PUBLIC;
create window quantile(val INTEGER, q DOUBLE) returns INTEGER
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(INTEGER, DOUBLE) TO PUBLIC;
create window quantile(val BIGINT, q DOUBLE) returns BIGINT
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(BIGINT, DOUBLE) TO PUBLIC;
create window quantile(val DECIMAL, q DOUBLE) returns DECIMAL
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(DECIMAL, DOUBLE) TO PUBLIC;
create window quantile(val REAL, q DOUBLE) returns REAL
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(REAL, DOUBLE) TO PUBLIC;
create window quantile(val DOUBLE, q DOUBLE) returns DOUBLE
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(DOUBLE, DOUBLE) TO PUBLIC;

create window quantile(val DATE, q DOUBLE) returns DATE
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(DATE, DOUBLE) TO PUBLIC;
create window quantile(val TIME, q DOUBLE) returns TIME
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(TIME, DOUBLE) TO PUBLIC;
create window quantile(val TIMESTAMP, q DOUBLE) returns TIMESTAMP
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(TIMESTAMP, DOUBLE) TO PUBLIC;
create window quantile(val INTERVAL SECOND, q DOUBLE) returns INTERVAL SECOND
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(INTERVAL SECOND, DOUBLE) TO PUBLIC;
create window quantile(val INTERVAL MONTH, q DOUBLE) returns INTERVAL MONTH
	external name "sql"."quantile";
GRANT EXECUTE ON WINDOW quantile(INTERVAL MONTH, DOUBLE) TO PUBLIC;


create aggregate median_avg(val TINYINT) returns DOUBLE
	external name "aggr"."median_avg";
GRANT EXECUTE ON AGGREGATE median_avg(TINYINT) TO PUBLIC;
create aggregate median_avg(val SMALLINT) returns DOUBLE
	external name "aggr"."median_avg";
GRANT EXECUTE ON AGGREGATE median_avg(SMALLINT) TO PUBLIC;
create aggregate median_avg(val INTEGER) returns DOUBLE
	external name "aggr"."median_avg";
GRANT EXECUTE ON AGGREGATE median_avg(INTEGER) TO PUBLIC;
create aggregate median_avg(val BIGINT) returns DOUBLE
	external name "aggr"."median_avg";
GRANT EXECUTE ON AGGREGATE median_avg(BIGINT) TO PUBLIC;
create aggregate median_avg(val DECIMAL) returns DOUBLE
	external name "aggr"."median_avg";
GRANT EXECUTE ON AGGREGATE median_avg(DECIMAL) TO PUBLIC;
create aggregate median_avg(val REAL) returns DOUBLE
	external name "aggr"."median_avg";
GRANT EXECUTE ON AGGREGATE median_avg(REAL) TO PUBLIC;
create aggregate median_avg(val DOUBLE) returns DOUBLE
	external name "aggr"."median_avg";
GRANT EXECUTE ON AGGREGATE median_avg(DOUBLE) TO PUBLIC;

create window median_avg(val TINYINT) returns DOUBLE
	external name "sql"."median_avg";
GRANT EXECUTE ON WINDOW median_avg(TINYINT) TO PUBLIC;
create window median_avg(val SMALLINT) returns DOUBLE
	external name "sql"."median_avg";
GRANT EXECUTE ON WINDOW median_avg(SMALLINT) TO PUBLIC;
create window median_avg(val INTEGER) returns DOUBLE
	external name "sql"."median_avg";
GRANT EXECUTE ON WINDOW median_avg(INTEGER) TO PUBLIC;
create window median_avg(val BIGINT) returns DOUBLE
	external name "sql"."median_avg";
GRANT EXECUTE ON WINDOW median_avg(BIGINT) TO PUBLIC;
create window median_avg(val DECIMAL) returns DOUBLE
	external name "sql"."median_avg";
GRANT EXECUTE ON WINDOW median_avg(DECIMAL) TO PUBLIC;
create window median_avg(val REAL) returns DOUBLE
	external name "sql"."median_avg";
GRANT EXECUTE ON WINDOW median_avg(REAL) TO PUBLIC;
create window median_avg(val DOUBLE) returns DOUBLE
	external name "sql"."median_avg";
GRANT EXECUTE ON WINDOW median_avg(DOUBLE) TO PUBLIC;


create aggregate quantile_avg(val TINYINT, q DOUBLE) returns DOUBLE
	external name "aggr"."quantile_avg";
GRANT EXECUTE ON AGGREGATE quantile_avg(TINYINT, DOUBLE) TO PUBLIC;
create aggregate quantile_avg(val SMALLINT, q DOUBLE) returns DOUBLE
	external name "aggr"."quantile_avg";
GRANT EXECUTE ON AGGREGATE quantile_avg(SMALLINT, DOUBLE) TO PUBLIC;
create aggregate quantile_avg(val INTEGER, q DOUBLE) returns DOUBLE
	external name "aggr"."quantile_avg";
GRANT EXECUTE ON AGGREGATE quantile_avg(INTEGER, DOUBLE) TO PUBLIC;
create aggregate quantile_avg(val BIGINT, q DOUBLE) returns DOUBLE
	external name "aggr"."quantile_avg";
GRANT EXECUTE ON AGGREGATE quantile_avg(BIGINT, DOUBLE) TO PUBLIC;
create aggregate quantile_avg(val DECIMAL, q DOUBLE) returns DOUBLE
	external name "aggr"."quantile_avg";
GRANT EXECUTE ON AGGREGATE quantile_avg(DECIMAL, DOUBLE) TO PUBLIC;
create aggregate quantile_avg(val REAL, q DOUBLE) returns DOUBLE
	external name "aggr"."quantile_avg";
GRANT EXECUTE ON AGGREGATE quantile_avg(REAL, DOUBLE) TO PUBLIC;
create aggregate quantile_avg(val DOUBLE, q DOUBLE) returns DOUBLE
	external name "aggr"."quantile_avg";
GRANT EXECUTE ON AGGREGATE quantile_avg(DOUBLE, DOUBLE) TO PUBLIC;


create aggregate corr(e1 TINYINT, e2 TINYINT) returns DOUBLE
	external name "aggr"."corr";
GRANT EXECUTE ON AGGREGATE corr(TINYINT, TINYINT) TO PUBLIC;
create aggregate corr(e1 SMALLINT, e2 SMALLINT) returns DOUBLE
	external name "aggr"."corr";
GRANT EXECUTE ON AGGREGATE corr(SMALLINT, SMALLINT) TO PUBLIC;
create aggregate corr(e1 INTEGER, e2 INTEGER) returns DOUBLE
	external name "aggr"."corr";
GRANT EXECUTE ON AGGREGATE corr(INTEGER, INTEGER) TO PUBLIC;
create aggregate corr(e1 BIGINT, e2 BIGINT) returns DOUBLE
	external name "aggr"."corr";
GRANT EXECUTE ON AGGREGATE corr(BIGINT, BIGINT) TO PUBLIC;
create aggregate corr(e1 REAL, e2 REAL) returns DOUBLE
	external name "aggr"."corr";
GRANT EXECUTE ON AGGREGATE corr(REAL, REAL) TO PUBLIC;
create aggregate corr(e1 DOUBLE, e2 DOUBLE) returns DOUBLE
	external name "aggr"."corr";
GRANT EXECUTE ON AGGREGATE corr(DOUBLE, DOUBLE) TO PUBLIC;
