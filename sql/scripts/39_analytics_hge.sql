-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

create aggregate stddev_samp(val HUGEINT) returns DOUBLE
	external name "aggr"."stdev";
GRANT EXECUTE ON AGGREGATE stddev_samp(HUGEINT) TO PUBLIC;
create window stddev_samp(val HUGEINT) returns DOUBLE
	external name "sql"."stdev";
GRANT EXECUTE ON WINDOW stddev_samp(HUGEINT) TO PUBLIC;

create aggregate stddev_pop(val HUGEINT) returns DOUBLE
	external name "aggr"."stdevp";
GRANT EXECUTE ON AGGREGATE stddev_pop(HUGEINT) TO PUBLIC;
create window stddev_pop(val HUGEINT) returns DOUBLE
	external name "sql"."stdevp";
GRANT EXECUTE ON WINDOW stddev_pop(HUGEINT) TO PUBLIC;

create aggregate var_samp(val HUGEINT) returns DOUBLE
	external name "aggr"."variance";
GRANT EXECUTE ON AGGREGATE var_samp(HUGEINT) TO PUBLIC;
create window var_samp(val HUGEINT) returns DOUBLE
	external name "sql"."variance";
GRANT EXECUTE ON WINDOW var_samp(HUGEINT) TO PUBLIC;

create aggregate covar_samp(e1 HUGEINT, e2 HUGEINT) returns DOUBLE
	external name "aggr"."covariance";
GRANT EXECUTE ON AGGREGATE covar_samp(HUGEINT, HUGEINT) TO PUBLIC;
create window covar_samp(e1 HUGEINT, e2 HUGEINT) returns DOUBLE
	external name "sql"."covariance";
GRANT EXECUTE ON WINDOW covar_samp(HUGEINT, HUGEINT) TO PUBLIC;

create aggregate var_pop(val HUGEINT) returns DOUBLE
	external name "aggr"."variancep";
GRANT EXECUTE ON AGGREGATE var_pop(HUGEINT) TO PUBLIC;
create window var_pop(val HUGEINT) returns DOUBLE
	external name "sql"."variancep";
GRANT EXECUTE ON WINDOW var_pop(HUGEINT) TO PUBLIC;

create aggregate covar_pop(e1 HUGEINT, e2 HUGEINT) returns DOUBLE
	external name "aggr"."covariancep";
GRANT EXECUTE ON AGGREGATE covar_pop(HUGEINT, HUGEINT) TO PUBLIC;
create window covar_pop(e1 HUGEINT, e2 HUGEINT) returns DOUBLE
	external name "sql"."covariancep";
GRANT EXECUTE ON WINDOW covar_pop(HUGEINT, HUGEINT) TO PUBLIC;

create aggregate median(val HUGEINT) returns HUGEINT
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(HUGEINT) TO PUBLIC;

create aggregate quantile(val HUGEINT, q DOUBLE) returns HUGEINT
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(HUGEINT, DOUBLE) TO PUBLIC;

create aggregate median_avg(val HUGEINT) returns DOUBLE
	external name "aggr"."median_avg";
GRANT EXECUTE ON AGGREGATE median_avg(HUGEINT) TO PUBLIC;

create aggregate quantile_avg(val HUGEINT, q DOUBLE) returns DOUBLE
	external name "aggr"."quantile_avg";
GRANT EXECUTE ON AGGREGATE quantile_avg(HUGEINT, DOUBLE) TO PUBLIC;

create aggregate corr(e1 HUGEINT, e2 HUGEINT) returns DOUBLE
	external name "aggr"."corr";
GRANT EXECUTE ON AGGREGATE corr(HUGEINT, HUGEINT) TO PUBLIC;
create window corr(e1 HUGEINT, e2 HUGEINT) returns DOUBLE
	external name "sql"."corr";
GRANT EXECUTE ON WINDOW corr(HUGEINT, HUGEINT) TO PUBLIC;
