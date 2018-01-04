-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

-- Author M.Kersten
-- This script gives the database administrator insight in the actual
-- value distribution over all tables in the database.


CREATE TABLE sys.statistics(
	"column_id" integer,
	"type" string,
	width integer,
	stamp timestamp,
	"sample" bigint,
	"count" bigint,
	"unique" bigint,
	"nils" bigint,
	minval string,
	maxval string,
	sorted boolean,
	revsorted boolean);

create procedure sys.analyze(minmax int, "sample" bigint)
external name sql.analyze;

create procedure sys.analyze(minmax int, "sample" bigint, sch string)
external name sql.analyze;

create procedure sys.analyze(minmax int, "sample" bigint, sch string, tbl string)
external name sql.analyze;

create procedure sys.analyze(minmax int, "sample" bigint, sch string, tbl string, col string)
external name sql.analyze;
