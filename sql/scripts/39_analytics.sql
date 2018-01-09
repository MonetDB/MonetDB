-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

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

create aggregate stddev_samp(val DATE) returns DOUBLE
	external name "aggr"."stdev";
GRANT EXECUTE ON AGGREGATE stddev_samp(DATE) TO PUBLIC;
create aggregate stddev_samp(val TIME) returns DOUBLE
	external name "aggr"."stdev";
GRANT EXECUTE ON AGGREGATE stddev_samp(TIME) TO PUBLIC;
create aggregate stddev_samp(val TIMESTAMP) returns DOUBLE
	external name "aggr"."stdev";
GRANT EXECUTE ON AGGREGATE stddev_samp(TIMESTAMP) TO PUBLIC;

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

create aggregate stddev_pop(val DATE) returns DOUBLE
	external name "aggr"."stdevp";
GRANT EXECUTE ON AGGREGATE stddev_pop(DATE) TO PUBLIC;
create aggregate stddev_pop(val TIME) returns DOUBLE
	external name "aggr"."stdevp";
GRANT EXECUTE ON AGGREGATE stddev_pop(TIME) TO PUBLIC;
create aggregate stddev_pop(val TIMESTAMP) returns DOUBLE
	external name "aggr"."stdevp";
GRANT EXECUTE ON AGGREGATE stddev_pop(TIMESTAMP) TO PUBLIC;

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

create aggregate var_samp(val DATE) returns DOUBLE
	external name "aggr"."variance";
GRANT EXECUTE ON AGGREGATE var_samp(DATE) TO PUBLIC;
create aggregate var_samp(val TIME) returns DOUBLE
	external name "aggr"."variance";
GRANT EXECUTE ON AGGREGATE var_samp(TIME) TO PUBLIC;
create aggregate var_samp(val TIMESTAMP) returns DOUBLE
	external name "aggr"."variance";
GRANT EXECUTE ON AGGREGATE var_samp(TIMESTAMP) TO PUBLIC;

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

create aggregate var_pop(val DATE) returns DOUBLE
	external name "aggr"."variancep";
GRANT EXECUTE ON AGGREGATE var_pop(DATE) TO PUBLIC;
create aggregate var_pop(val TIME) returns DOUBLE
	external name "aggr"."variancep";
GRANT EXECUTE ON AGGREGATE var_pop(TIME) TO PUBLIC;
create aggregate var_pop(val TIMESTAMP) returns DOUBLE
	external name "aggr"."variancep";
GRANT EXECUTE ON AGGREGATE var_pop(TIMESTAMP) TO PUBLIC;

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
