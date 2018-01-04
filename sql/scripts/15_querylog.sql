-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

-- QUERY HISTORY
-- The query history mechanism of MonetDB/SQL relies on a few hooks.
-- The most important one is a global system variable which controls
--  monitoring of all sessions.

create function sys.querylog_catalog()
returns table(
	id oid,
	owner string,
	defined timestamp,
	query string,
	pipe string,
	"plan" string,		-- Name of MAL plan
	mal int,		-- size of MAL plan
	optimize bigint	-- time in usec
)
external name sql.querylog_catalog;

-- Each query call is stored in the table calls
-- At regular intervals the query history table should be cleaned.
-- This can be done manually on the SQL console, or be integrated
-- in the keepQuery and keepCall upon need.
-- The parameters are geared at understanding the resource claims
-- They reflect the effect of the total workload mix during execution.
-- The 'cpu' gives the average cpu load percentage over all cores on the
-- server during execution phase.
-- increasing cpu load indicates better use of multi-cores.
-- The 'io' indicate IOs during complete query run.
-- The 'space' is the total amount of intermediates created in MB.
-- Reducing the space component improves performance/
-- All timing in usec and all storage in bytes.

create function sys.querylog_calls()
returns table(
	id oid,		 -- references query plan
	"start" timestamp,	-- time the statement was started
	"stop" timestamp,	-- time the statement was completely finished
	arguments string,	-- actual call structure
	tuples bigint,		-- number of tuples in the result set
	run bigint,		-- time spent (in usec)  until the result export
	ship bigint,		-- time spent (in usec)  to ship the result set
	cpu int,		-- average cpu load percentage during execution
	io int			-- percentage time waiting for IO to finish
)
external name sql.querylog_calls;

-- create table views for convenience
create view sys.querylog_catalog as select * from sys.querylog_catalog();
create view sys.querylog_calls as select * from sys.querylog_calls();
create view sys.querylog_history as
select qd.*, ql."start",ql."stop", ql.arguments, ql.tuples, ql.run, ql.ship, ql.cpu, ql.io
from sys.querylog_catalog() qd, sys.querylog_calls() ql
where qd.id = ql.id and qd.owner = user;

-- reset history for a particular user
create procedure sys.querylog_empty()
external name sql.querylog_empty;

-- manipulate the query logger
create procedure sys.querylog_enable()
external name sql.querylog_enable;
create procedure sys.querylog_enable(threshold smallint)
external name sql.querylog_enable_threshold;
create procedure sys.querylog_disable()
external name sql.querylog_disable;
