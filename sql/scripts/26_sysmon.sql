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
-- Copyright August 2008-2014 MonetDB B.V.
-- All Rights Reserved.

-- System monitoring

-- show status of all active SQL queries.
create function sys.queue()
returns table(
	qtag bigint,
	"user" string,
	started timestamp,
	estimate timestamp,
	progress int,
	status string,
	tag oid,
	query string
)
external name sql.sysmon_queue;

create view sys.queue as select * from sys.queue();
update sys._tables
    set system = true
    where name = 'queue'
        and schema_id = (select id from sys.schemas where name = 'sys');

-- operations to manipulate the state of havoc queries
create procedure sys.pause(tag int)
external name sql.sysmon_pause;
create procedure sys.resume(tag int)
external name sql.sysmon_resume;
create procedure sys.stop(tag int)
external name sql.sysmon_stop;

create procedure sys.pause(tag bigint)
external name sql.sysmon_pause;
create procedure sys.resume(tag bigint)
external name sql.sysmon_resume;
create procedure sys.stop(tag bigint)
external name sql.sysmon_stop;

--create function sysmon.connections()
--returns table(
--)
--external name sql.sql_sysmon_connections;
