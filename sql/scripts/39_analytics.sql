-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

create aggregate stddev_samp(val TINYINT) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev_samp(val SMALLINT) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev_samp(val INTEGER) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev_samp(val WRD) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev_samp(val BIGINT) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev_samp(val REAL) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev_samp(val DOUBLE) returns DOUBLE
	external name "aggr"."stdev";

create aggregate stddev_samp(val DATE) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev_samp(val TIME) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev_samp(val TIMESTAMP) returns DOUBLE
	external name "aggr"."stdev";

create aggregate stddev_pop(val TINYINT) returns DOUBLE
	external name "aggr"."stdevp";
create aggregate stddev_pop(val SMALLINT) returns DOUBLE
	external name "aggr"."stdevp";
create aggregate stddev_pop(val INTEGER) returns DOUBLE
	external name "aggr"."stdevp";
create aggregate stddev_pop(val WRD) returns DOUBLE
	external name "aggr"."stdevp";
create aggregate stddev_pop(val BIGINT) returns DOUBLE
	external name "aggr"."stdevp";
create aggregate stddev_pop(val REAL) returns DOUBLE
	external name "aggr"."stdevp";
create aggregate stddev_pop(val DOUBLE) returns DOUBLE
	external name "aggr"."stdevp";

create aggregate stddev_pop(val DATE) returns DOUBLE
	external name "aggr"."stdevp";
create aggregate stddev_pop(val TIME) returns DOUBLE
	external name "aggr"."stdevp";
create aggregate stddev_pop(val TIMESTAMP) returns DOUBLE
	external name "aggr"."stdevp";

create aggregate var_samp(val TINYINT) returns DOUBLE
	external name "aggr"."variance";
create aggregate var_samp(val SMALLINT) returns DOUBLE
	external name "aggr"."variance";
create aggregate var_samp(val INTEGER) returns DOUBLE
	external name "aggr"."variance";
create aggregate var_samp(val WRD) returns DOUBLE
	external name "aggr"."variance";
create aggregate var_samp(val BIGINT) returns DOUBLE
	external name "aggr"."variance";
create aggregate var_samp(val REAL) returns DOUBLE
	external name "aggr"."variance";
create aggregate var_samp(val DOUBLE) returns DOUBLE
	external name "aggr"."variance";

create aggregate var_samp(val DATE) returns DOUBLE
	external name "aggr"."variance";
create aggregate var_samp(val TIME) returns DOUBLE
	external name "aggr"."variance";
create aggregate var_samp(val TIMESTAMP) returns DOUBLE
	external name "aggr"."variance";

create aggregate var_pop(val TINYINT) returns DOUBLE
	external name "aggr"."variancep";
create aggregate var_pop(val SMALLINT) returns DOUBLE
	external name "aggr"."variancep";
create aggregate var_pop(val INTEGER) returns DOUBLE
	external name "aggr"."variancep";
create aggregate var_pop(val WRD) returns DOUBLE
	external name "aggr"."variancep";
create aggregate var_pop(val BIGINT) returns DOUBLE
	external name "aggr"."variancep";
create aggregate var_pop(val REAL) returns DOUBLE
	external name "aggr"."variancep";
create aggregate var_pop(val DOUBLE) returns DOUBLE
	external name "aggr"."variancep";

create aggregate var_pop(val DATE) returns DOUBLE
	external name "aggr"."variancep";
create aggregate var_pop(val TIME) returns DOUBLE
	external name "aggr"."variancep";
create aggregate var_pop(val TIMESTAMP) returns DOUBLE
	external name "aggr"."variancep";

create aggregate median(val TINYINT) returns TINYINT
	external name "aggr"."median";
create aggregate median(val SMALLINT) returns SMALLINT
	external name "aggr"."median";
create aggregate median(val INTEGER) returns INTEGER
	external name "aggr"."median";
create aggregate median(val WRD) returns WRD
	external name "aggr"."median";
create aggregate median(val BIGINT) returns BIGINT
	external name "aggr"."median";
create aggregate median(val DECIMAL) returns DECIMAL
	external name "aggr"."median";
create aggregate median(val REAL) returns REAL
	external name "aggr"."median";
create aggregate median(val DOUBLE) returns DOUBLE
	external name "aggr"."median";

create aggregate median(val DATE) returns DATE
	external name "aggr"."median";
create aggregate median(val TIME) returns TIME
	external name "aggr"."median";
create aggregate median(val TIMESTAMP) returns TIMESTAMP
	external name "aggr"."median";

create aggregate quantile(val TINYINT, q DOUBLE) returns TINYINT
	external name "aggr"."quantile";
create aggregate quantile(val SMALLINT, q DOUBLE) returns SMALLINT
	external name "aggr"."quantile";
create aggregate quantile(val INTEGER, q DOUBLE) returns INTEGER
	external name "aggr"."quantile";
create aggregate quantile(val WRD, q DOUBLE) returns WRD
	external name "aggr"."quantile";
create aggregate quantile(val BIGINT, q DOUBLE) returns BIGINT
	external name "aggr"."quantile";
create aggregate quantile(val DECIMAL, q DOUBLE) returns DECIMAL
	external name "aggr"."quantile";
create aggregate quantile(val REAL, q DOUBLE) returns REAL
	external name "aggr"."quantile";
create aggregate quantile(val DOUBLE, q DOUBLE) returns DOUBLE
	external name "aggr"."quantile";


create aggregate quantile(val DATE, q DOUBLE) returns DATE
	external name "aggr"."quantile";
create aggregate quantile(val TIME, q DOUBLE) returns TIME
	external name "aggr"."quantile";
create aggregate quantile(val TIMESTAMP, q DOUBLE) returns TIMESTAMP
	external name "aggr"."quantile";

create aggregate corr(e1 TINYINT, e2 TINYINT) returns TINYINT
	external name "aggr"."corr";
create aggregate corr(e1 SMALLINT, e2 SMALLINT) returns SMALLINT
	external name "aggr"."corr";
create aggregate corr(e1 INTEGER, e2 INTEGER) returns INTEGER
	external name "aggr"."corr";
create aggregate corr(e1 WRD, e2 WRD) returns WRD
	external name "aggr"."corr";
create aggregate corr(e1 BIGINT, e2 BIGINT) returns BIGINT
	external name "aggr"."corr";
create aggregate corr(e1 REAL, e2 REAL) returns REAL
	external name "aggr"."corr";
create aggregate corr(e1 DOUBLE, e2 DOUBLE) returns DOUBLE
	external name "aggr"."corr";
