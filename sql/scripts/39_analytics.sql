-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

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


create aggregate covar_samp(e1 TINYINT, e2 TINYINT) returns DOUBLE
	external name "aggr"."covariance";
GRANT EXECUTE ON AGGREGATE covar_samp(TINYINT, TINYINT) TO PUBLIC;
create aggregate covar_samp(e1 SMALLINT, e2 SMALLINT) returns DOUBLE
	external name "aggr"."covariance";
GRANT EXECUTE ON AGGREGATE covar_samp(SMALLINT, SMALLINT) TO PUBLIC;
create aggregate covar_samp(e1 INTEGER, e2 INTEGER) returns DOUBLE
	external name "aggr"."covariance";
GRANT EXECUTE ON AGGREGATE covar_samp(INTEGER, INTEGER) TO PUBLIC;
create aggregate covar_samp(e1 BIGINT, e2 BIGINT) returns DOUBLE
	external name "aggr"."covariance";
GRANT EXECUTE ON AGGREGATE covar_samp(BIGINT, BIGINT) TO PUBLIC;
create aggregate covar_samp(e1 REAL, e2 REAL) returns DOUBLE
	external name "aggr"."covariance";
GRANT EXECUTE ON AGGREGATE covar_samp(REAL, REAL) TO PUBLIC;
create aggregate covar_samp(e1 DOUBLE, e2 DOUBLE) returns DOUBLE
	external name "aggr"."covariance";
GRANT EXECUTE ON AGGREGATE covar_samp(DOUBLE, DOUBLE) TO PUBLIC;

create window covar_samp(e1 TINYINT, e2 TINYINT) returns DOUBLE
	external name "sql"."covariance";
GRANT EXECUTE ON WINDOW covar_samp(TINYINT, TINYINT) TO PUBLIC;
create window covar_samp(e1 SMALLINT, e2 SMALLINT) returns DOUBLE
	external name "sql"."covariance";
GRANT EXECUTE ON WINDOW covar_samp(SMALLINT, SMALLINT) TO PUBLIC;
create window covar_samp(e1 INTEGER, e2 INTEGER) returns DOUBLE
	external name "sql"."covariance";
GRANT EXECUTE ON WINDOW covar_samp(INTEGER, INTEGER) TO PUBLIC;
create window covar_samp(e1 BIGINT, e2 BIGINT) returns DOUBLE
	external name "sql"."covariance";
GRANT EXECUTE ON WINDOW covar_samp(BIGINT, BIGINT) TO PUBLIC;
create window covar_samp(e1 REAL, e2 REAL) returns DOUBLE
	external name "sql"."covariance";
GRANT EXECUTE ON WINDOW covar_samp(REAL, REAL) TO PUBLIC;
create window covar_samp(e1 DOUBLE, e2 DOUBLE) returns DOUBLE
	external name "sql"."covariance";
GRANT EXECUTE ON WINDOW covar_samp(DOUBLE, DOUBLE) TO PUBLIC;


create aggregate covar_pop(e1 TINYINT, e2 TINYINT) returns DOUBLE
	external name "aggr"."covariancep";
GRANT EXECUTE ON AGGREGATE covar_pop(TINYINT, TINYINT) TO PUBLIC;
create aggregate covar_pop(e1 SMALLINT, e2 SMALLINT) returns DOUBLE
	external name "aggr"."covariancep";
GRANT EXECUTE ON AGGREGATE covar_pop(SMALLINT, SMALLINT) TO PUBLIC;
create aggregate covar_pop(e1 INTEGER, e2 INTEGER) returns DOUBLE
	external name "aggr"."covariancep";
GRANT EXECUTE ON AGGREGATE covar_pop(INTEGER, INTEGER) TO PUBLIC;
create aggregate covar_pop(e1 BIGINT, e2 BIGINT) returns DOUBLE
	external name "aggr"."covariancep";
GRANT EXECUTE ON AGGREGATE covar_pop(BIGINT, BIGINT) TO PUBLIC;
create aggregate covar_pop(e1 REAL, e2 REAL) returns DOUBLE
	external name "aggr"."covariancep";
GRANT EXECUTE ON AGGREGATE covar_pop(REAL, REAL) TO PUBLIC;
create aggregate covar_pop(e1 DOUBLE, e2 DOUBLE) returns DOUBLE
	external name "aggr"."covariancep";
GRANT EXECUTE ON AGGREGATE covar_pop(DOUBLE, DOUBLE) TO PUBLIC;

create window covar_pop(e1 TINYINT, e2 TINYINT) returns DOUBLE
	external name "sql"."covariancep";
GRANT EXECUTE ON WINDOW covar_pop(TINYINT, TINYINT) TO PUBLIC;
create window covar_pop(e1 SMALLINT, e2 SMALLINT) returns DOUBLE
	external name "sql"."covariancep";
GRANT EXECUTE ON WINDOW covar_pop(SMALLINT, SMALLINT) TO PUBLIC;
create window covar_pop(e1 INTEGER, e2 INTEGER) returns DOUBLE
	external name "sql"."covariancep";
GRANT EXECUTE ON WINDOW covar_pop(INTEGER, INTEGER) TO PUBLIC;
create window covar_pop(e1 BIGINT, e2 BIGINT) returns DOUBLE
	external name "sql"."covariancep";
GRANT EXECUTE ON WINDOW covar_pop(BIGINT, BIGINT) TO PUBLIC;
create window covar_pop(e1 REAL, e2 REAL) returns DOUBLE
	external name "sql"."covariancep";
GRANT EXECUTE ON WINDOW covar_pop(REAL, REAL) TO PUBLIC;
create window covar_pop(e1 DOUBLE, e2 DOUBLE) returns DOUBLE
	external name "sql"."covariancep";
GRANT EXECUTE ON WINDOW covar_pop(DOUBLE, DOUBLE) TO PUBLIC;


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
create aggregate median(val INTERVAL DAY) returns INTERVAL DAY
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(INTERVAL DAY) TO PUBLIC;
create aggregate median(val INTERVAL MONTH) returns INTERVAL MONTH
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(INTERVAL MONTH) TO PUBLIC;


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
create aggregate quantile(val INTERVAL DAY, q DOUBLE) returns INTERVAL DAY
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(INTERVAL DAY, DOUBLE) TO PUBLIC;
create aggregate quantile(val INTERVAL MONTH, q DOUBLE) returns INTERVAL MONTH
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(INTERVAL MONTH, DOUBLE) TO PUBLIC;


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

create window corr(e1 TINYINT, e2 TINYINT) returns DOUBLE
	external name "sql"."corr";
GRANT EXECUTE ON WINDOW corr(TINYINT, TINYINT) TO PUBLIC;
create window corr(e1 SMALLINT, e2 SMALLINT) returns DOUBLE
	external name "sql"."corr";
GRANT EXECUTE ON WINDOW corr(SMALLINT, SMALLINT) TO PUBLIC;
create window corr(e1 INTEGER, e2 INTEGER) returns DOUBLE
	external name "sql"."corr";
GRANT EXECUTE ON WINDOW corr(INTEGER, INTEGER) TO PUBLIC;
create window corr(e1 BIGINT, e2 BIGINT) returns DOUBLE
	external name "sql"."corr";
GRANT EXECUTE ON WINDOW corr(BIGINT, BIGINT) TO PUBLIC;
create window corr(e1 REAL, e2 REAL) returns DOUBLE
	external name "sql"."corr";
GRANT EXECUTE ON WINDOW corr(REAL, REAL) TO PUBLIC;
create window corr(e1 DOUBLE, e2 DOUBLE) returns DOUBLE
	external name "sql"."corr";
GRANT EXECUTE ON WINDOW corr(DOUBLE, DOUBLE) TO PUBLIC;
