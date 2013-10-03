create aggregate stddev_samp(val HUGEINT) returns DOUBLE
	external name "aggr"."stdev";
create aggregate stddev_pop(val HUGEINT) returns DOUBLE
	external name "aggr"."stdevp";
create aggregate var_samp(val HUGEINT) returns DOUBLE
	external name "aggr"."variance";
create aggregate var_pop(val HUGEINT) returns DOUBLE
	external name "aggr"."variancep";
create aggregate median(val HUGEINT) returns HUGEINT
	external name "aggr"."median";
create aggregate corr(e1 HUGEINT, e2 HUGEINT) returns HUGEINT
	external name "aggr"."corr";
