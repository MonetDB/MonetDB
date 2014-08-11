-- The contents of this file are subject to the MonetDB Public License
-- Version 1.1 (the "License"); you may not use this file except in
-- compliance with the License. You may obtain a copy of the License at
-- http://www.monetdb.org/Legal/MonetDBLicense
--
-- Software distributed under the License is distributed on an "AS IS"
-- basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
-- License for the specific language governing rights and limitations
-- under the License.
--
-- The Original Code is the MonetDB Database System.
--
-- The Initial Developer of the Original Code is CWI.
-- Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
-- Copyright August 2008-2014 MonetDB B.V.
-- All Rights Reserved.

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
