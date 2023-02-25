-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.

-- System monitoring

-- show own status of all active SQL queries.
create function sys.queue()
returns table(
	"tag" bigint,
	"sessionid" int,
	"username" string,
	"started" timestamp,
	"status" string,	-- paused, running, finished
	"query" string,
	"finished" timestamp,	
	"maxworkers" int,	-- maximum number of concurrent worker threads
	"footprint" int		-- maximum memory claim awarded
)
external name sysmon.queue;
grant execute on function sys.queue() to public;
create view sys.queue as select * from sys.queue();
grant select on sys.queue to public;
-- sysadmin show query queue of all users or a specific user.
create function sys.queue(username string)
returns table(
	"tag" bigint,
	"sessionid" int,
	"username" string,
	"started" timestamp,
	"status" string,	-- paused, running, finished
	"query" string,
	"finished" timestamp,	
	"maxworkers" int,	-- maximum number of concurrent worker threads
	"footprint" int		-- maximum memory claim awarded
)
external name sysmon.queue;

-- user owned operations to manipulate queries
create procedure sys.pause(tag bigint)
external name sysmon.pause;
grant execute on procedure sys.pause(bigint) to public;
create procedure sys.resume(tag bigint)
external name sysmon.resume;
grant execute on procedure sys.resume(bigint) to public;
create procedure sys.stop(tag bigint)
external name sysmon.stop;
grant execute on procedure sys.stop(bigint) to public;
-- sysadmin operations to manipulate queries
create procedure sys.pause(tag bigint, username string)
external name sysmon.pause;
create procedure sys.resume(tag bigint, username string)
external name sysmon.resume;
create procedure sys.stop(tag bigint, username string)
external name sysmon.stop;

-- we collect some aggregated user information
create function sys.user_statistics()
returns table(
	username string,
	querycount bigint,
	totalticks bigint,
	started timestamp,
	finished timestamp,
	maxticks bigint,
	maxquery string
)
external name sysmon.user_statistics;

create procedure sys.vacuum(sname string, tname string, cname string)
external name sql.vacuum;
create procedure sys.vacuum(sname string, tname string, cname string, interval int)
external name sql.vacuum;
create procedure sys.stop_vacuum(sname string, tname string, cname string)
external name sql.stop_vacuum;
