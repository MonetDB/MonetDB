-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.

create function sys.password_hash (username string)
	returns string
	external name sql.password;

create function sys.remote_table_credentials (tablename string)
returns table ("uri" string, "username" string, "hash" string)
external name sql.rt_credentials;

create function sys.sessions()
returns table("sessionid" int, "user" string, "login" timestamp, "sessiontimeout" bigint, "lastcommand" timestamp, "querytimeout" bigint, "threads" int)
external name sql.sessions;
create view sys.sessions as select * from sys.sessions();

-- routines to bring the system down quickly
create procedure sys.shutdown(delay tinyint)
external name sql.shutdown;

create procedure sys.shutdown(delay tinyint, force bool)
external name sql.shutdown;

-- control the query and session time out for the current user.
-- measured in seconds
create procedure sys.settimeout("query" bigint)
	external name clients.settimeout;

create procedure sys.setsession("timeout" bigint)
	external name clients.setsession;

-- The super user can change the properties of all sessions
create procedure sys.querytimeout("sessionid" int, "query" bigint)
    external name clients.querytimeout;

create procedure sys.sessiontimeout("sessionid" int, "query" bigint)
    external name clients.sessiontimeout;

create procedure sys.stopsession("sessionid" int)
    external name clients.stopsession;

create procedure sys.setprinttimeout("timeout" integer)
	external name clients.setprinttimeout;
