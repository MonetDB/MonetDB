-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

-- show the optimizer statistics maintained by the SQL frontend
create function sys.optimizer_stats()
	returns table (optname string, count int, timing bigint)
	external name inspect.optimizer_stats;


-- SQL QUERY CACHE
-- The SQL query cache returns a table with the query plans kept

create function sys.queryCache()
	returns table (query string, count int)
	external name sql.dump_cache;

-- Trace the SQL input
create procedure sys.querylog(filename string)
	external name sql.logfile;

-- MONETDB KERNEL SECTION
-- optimizer pipe catalog
create function sys.optimizers ()
	returns table (name string, def string, status string)
	external name sql.optimizers;
create view sys.optimizers as select * from sys.optimizers();

-- The environment table
create view sys.environment as select * from sys.env();
GRANT SELECT ON sys.environment TO PUBLIC;

-- The BAT buffer pool overview
create function sys.bbp ()
	returns table (id int, name string,
		ttype string, count BIGINT, refcnt int, lrefcnt int,
		location string, heat int, dirty string,
		status string, kind string)
	external name bbp.get;

create function sys.malfunctions()
	returns table("module" string, "function" string, "signature" string, "address" string, "comment" string)
	external name "manual"."functions";

create procedure sys.evalAlgebra( ra_stmt string, opt bool)
	external name sql."evalAlgebra";

-- enqueue a flush log, ie as soon as no transactions are active
-- flush the log and cleanup the used storage
/*
create procedure sys.flush_log ()
	external name sql."flush_log";
*/

-- Helper function to disable the log merger
create procedure sys.suspend_log_flushing()
	external name sql.suspend_log_flushing;

-- Helper function to enable the log merger
create procedure sys.resume_log_flushing()
	external name sql.resume_log_flushing;

create function sys.debug(debug int) returns integer
	external name mdb."setDebug";

create function sys.debug(flag string) returns integer
	external name mdb."setDebug";

create function sys.debugflags()
	returns table(flag string, val bool)
	external name mdb."getDebugFlags";

create function sys.deltas ("schema" string)
	returns table ("id" int, "cleared" boolean, "immutable" bigint, "inserted" bigint, "updates" bigint, "deletes" bigint, "level" int)
	external name "sql"."deltas";

create function sys.deltas ("schema" string, "table" string)
	returns table ("id" int, "cleared" boolean, "immutable" bigint, "inserted" bigint, "updates" bigint, "deletes" bigint, "level" int)
	external name "sql"."deltas";

create function sys.deltas ("schema" string, "table" string, "column" string)
	returns table ("id" int, "cleared" boolean, "immutable" bigint, "inserted" bigint, "updates" bigint, "deletes" bigint, "level" int)
	external name "sql"."deltas";
