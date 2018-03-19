-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

create aggregate stddev_samp(val HUGEINT) returns DOUBLE
	external name "aggr"."stdev";
GRANT EXECUTE ON AGGREGATE stddev_samp(HUGEINT) TO PUBLIC;
create aggregate stddev_pop(val HUGEINT) returns DOUBLE
	external name "aggr"."stdevp";
GRANT EXECUTE ON AGGREGATE stddev_pop(HUGEINT) TO PUBLIC;
create aggregate var_samp(val HUGEINT) returns DOUBLE
	external name "aggr"."variance";
GRANT EXECUTE ON AGGREGATE var_samp(HUGEINT) TO PUBLIC;
create aggregate var_pop(val HUGEINT) returns DOUBLE
	external name "aggr"."variancep";
GRANT EXECUTE ON AGGREGATE var_pop(HUGEINT) TO PUBLIC;
create aggregate median(val HUGEINT) returns HUGEINT
	external name "aggr"."median";
GRANT EXECUTE ON AGGREGATE median(HUGEINT) TO PUBLIC;
create aggregate quantile(val HUGEINT, q DOUBLE) returns HUGEINT
	external name "aggr"."quantile";
GRANT EXECUTE ON AGGREGATE quantile(HUGEINT, DOUBLE) TO PUBLIC;
create aggregate corr(e1 HUGEINT, e2 HUGEINT) returns DOUBLE
	external name "aggr"."corr";
GRANT EXECUTE ON AGGREGATE corr(HUGEINT, HUGEINT) TO PUBLIC;
