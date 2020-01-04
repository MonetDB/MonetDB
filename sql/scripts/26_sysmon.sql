-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

-- System monitoring

-- show status of all active SQL queries.
create function sys.queue()
returns table(
	"tag" bigint,
	"sessionid" int,
	"username" string,
	"started" timestamp,
	"status" string,	-- paused, running
	"query" string,
	"progress" int,	-- percentage of MAL instructions handled
	"workers" int,
	"memory" int
)
external name sql.sysmon_queue;
grant execute on function sys.queue to public;

create view sys.queue as select * from sys.queue();
grant select on sys.queue to public;

-- operations to manipulate the state of havoc queries
create procedure sys.pause(tag tinyint)
external name sql.sysmon_pause;
create procedure sys.resume(tag tinyint)
external name sql.sysmon_resume;
create procedure sys.stop(tag tinyint)
external name sql.sysmon_stop;

create procedure sys.pause(tag smallint)
external name sql.sysmon_pause;
create procedure sys.resume(tag smallint)
external name sql.sysmon_resume;
create procedure sys.stop(tag smallint)
external name sql.sysmon_stop;

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
