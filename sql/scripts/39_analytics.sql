create aggregate stddev(val TINYINT) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev(val SMALLINT) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev(val INTEGER) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev(val BIGINT) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev(val REAL) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev(val DOUBLE) returns DOUBLE
	external name "aggr"."stdev";

create aggregate stddev(val DATE) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev(val TIME) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev(val TIMESTAMP) returns DOUBLE
	external name "aggr"."stdev";

create aggregate stddev_pop(val TINYINT) returns DOUBLE
	external name "aggr"."stdevp";
create aggregate stddev_pop(val SMALLINT) returns DOUBLE
	external name "aggr"."stdevp";
create aggregate stddev_pop(val INTEGER) returns DOUBLE
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
create aggregate median(val BIGINT) returns BIGINT
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

create aggregate corr(e1 TINYINT, e2 TINYINT) returns TINYINT
	external name "aggr"."corr";
create aggregate corr(e1 SMALLINT, e2 SMALLINT) returns SMALLINT
	external name "aggr"."corr";
create aggregate corr(e1 INTEGER, e2 INTEGER) returns INTEGER
	external name "aggr"."corr";
create aggregate corr(e1 BIGINT, e2 BIGINT) returns BIGINT
	external name "aggr"."corr";
create aggregate corr(e1 REAL, e2 REAL) returns REAL
	external name "aggr"."corr";
create aggregate corr(e1 DOUBLE, e2 DOUBLE) returns DOUBLE
	external name "aggr"."corr";
