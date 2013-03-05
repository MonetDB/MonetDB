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
-- The query history mechanism of MonetDB/SQL relies on a few hooks 
-- inside the kernel. The most important one is the SQL global 
-- variable 'history', which is used by all sessions.
-- It is set automatically at the end of this script.

-- Whenever a query is compiled and added to the cache, it is also entered
-- into the 'queryHistory' table using a hardwired call to 'keepQuery'.

create table queryHistory(
	id wrd primary key,
	defined timestamp,	-- when entered into the cache
	name string,		-- database user name
	query string,
	parse bigint,		-- time in usec
	optimize bigint 	-- time in usec
);
update _tables
	set system = true
	where name = 'queryhistory'
		and schema_id = (select id from schemas where name = 'sys');

-- Each query call is stored in the table callHistory using 'keepCall'.
-- At regular intervals the query history table should be cleaned.
-- This can be done manually on the SQL console, or be integrated
-- in the keepQuery and keepCall upon need.
-- The parameters are geared at understanding the resource claims
-- The 'foot'-print depicts the maximum amount of memory used to keep all
-- relevant intermediates and persistent bats in memory at any time
-- during query execution.
-- The 'memory' parameter is total amount of BAT storage claimed during
-- query execution.
-- The 'inblock' and 'oublock' indicate the physical IOs during.
-- All timing in usec and all storage in bytes.

create table callHistory(
	id wrd references queryHistory(id), -- references query plan
	ctime timestamp,	-- time the first statement was executed
	arguments string,
	exec bigint,		-- time from the first statement until result export
	result bigint,		-- time to ship the result to the client
	foot bigint, 		-- footprint for all bats in the plan
	memory bigint,		-- storage size of intermediates created
	tuples wrd,			-- number of tuples in the result set
	inblock bigint,		-- number of physical blocks read
	oublock bigint		-- number of physical blocks written
);
update _tables
	set system = true
	where name = 'callhistory'
		and schema_id = (select id from schemas where name = 'sys');

create view queryLog as
select qd.*, ql.ctime, ql.arguments, ql.exec, ql.result, ql.foot, ql.memory, ql.tuples, ql.inblock, ql.oublock from queryHistory qd, callHistory ql
where qd.id = ql.id;
update _tables
	set system = true
	where name = 'querylog'
		and schema_id = (select id from schemas where name = 'sys');

-- the signature is used in the kernel, don't change it
-- make the call visible in the query log as soon as it is started
create procedure keepQuery(
	i wrd,
	q string,
	parse bigint,
	optimize bigint) 
begin
	declare b boolean;
	set b = (select count(*) = 0 from queryHistory qh where qh.query = q);
	if (b)
	then
		insert into queryHistory values(i, now(), user, q, parse, optimize);
	end if;
	insert into callHistory
	values( i, now(), null, null, null, null, null, null, null, null );
end;

-- the signature is used in the kernel, don't change it
create procedure keepCall(
	idx wrd, 			-- references query plan
	ctx timestamp,		-- time the first statement was executed
	arg string,
	xtime bigint,		-- time from the first statement until result export
	rtime bigint,		-- time to ship the result to the client
	foot bigint, 		-- footprint for all bats in the plan
	memory bigint,		-- storage size of intermediates created
	tuples wrd,			-- number of tuples in the result set
	inblock bigint,		-- number of physical blocks read
	oublock bigint		-- number of physical blocks written
)
begin
	declare b boolean;
	set b = (select count(*) > 0 from callHistory where id = idx and arguments is null);
	if (b)
	then
		delete from callHistory where id = idx and arguments is null;
	end if;
	insert into callHistory
	values( idx, ctx, arg, xtime, rtime, foot, memory, tuples, inblock, oublock );
end;

create procedure resetHistory()
begin
	delete from callHistory;
	delete from queryHistory;
end;

-- set history=true;
