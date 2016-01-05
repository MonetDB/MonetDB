-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

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
create aggregate quantile(val HUGEINT, q DOUBLE) returns HUGEINT
	external name "aggr"."quantile";
create aggregate corr(e1 HUGEINT, e2 HUGEINT) returns HUGEINT
	external name "aggr"."corr";
