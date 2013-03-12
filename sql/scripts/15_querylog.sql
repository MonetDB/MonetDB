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
-- Copyright August 2008-2013 MonetDB B.V.
-- All Rights Reserved.

-- QUERY HISTORY
-- The query history mechanism of MonetDB/SQL relies on a few hooks.
-- The most important one is a global system variable which controls
--  monitoring of all sessions. 

create schema querylog;

create function querylog.catalog()
returns table(
	id oid,
	owner string,
	defined timestamp,
	query string,
	pipe string,
	mal int,			-- size of MAL plan
	optimize bigint 	-- time in usec
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

create function querylog.calls()
returns table(
	id oid,				 -- references query plan
	"start" timestamp,	-- time the statement was started
	"stop" timestamp,	-- time the statement was completely finished
	arguments string,	-- actual call structure
	tuples wrd,			-- number of tuples in the result set
	run bigint,		-- time spent (in usec)  until the result export
	ship bigint,		-- time spent (in usec)  to ship the result set
	cpu int,  		-- average cpu load percentage during execution
	io int,			-- percentage time waiting for IO to finish 
	space bigint		-- total storage size of intermediates created (in MB)
)
external name sql.querylog_calls;

create view querylog.history as
select qd.*, ql."start",ql."stop", ql.arguments, ql.tuples, ql.run, ql.ship, ql.cpu, ql.space, ql.io 
from querylog.catalog() qd, querylog.calls() ql
where qd.id = ql.id and qd.owner = user;

-- reset history for a particular user
create procedure querylog.reset()
external name sql.querylog_reset;

-- manipulate the query logger
create procedure querylog.init()
external name sql.querylog_init;
create procedure querylog.init(threshold smallint)
external name sql.querylog_init_threshold;
create procedure querylog.done()
external name sql.querylog_done;
