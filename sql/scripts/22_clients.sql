-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2024 MonetDB Foundation;
-- Copyright August 2008 - 2023 MonetDB B.V.;
-- Copyright 1997 - July 2008 CWI.

create function sys.password_hash (username string)
	returns string
    external name sql.password;
	-- return select password from users where name = username;

create function sys.decypher (cypher string)
	returns string
    external name sql.decypher;
	-- return decyphered pwhash 

create function sys.sessions()
returns table(
	"sessionid" int,
	"username" string,
	"login" timestamp,
	"idle" timestamp,
	"optimizer" string,
	"sessiontimeout" int,
	"querytimeout" int,
	"workerlimit" int,
	"memorylimit" int
)
external name sql.sessions;
create view sys.sessions as select * from sys.sessions();
-- we won't grant sys.sessions to the public

-- routines to bring the system down quickly
create procedure sys.shutdown(delay tinyint)
	external name sql.shutdown;
-- we won't grant sys.shutdown to the public
create procedure sys.shutdown(delay tinyint, force bool)
	external name sql.shutdown;
-- we won't grant sys.shutdown to the public

-- control the session properties  session time out for the current user.
create procedure sys.setoptimizer("optimizer" string)
	external name clients.setoptimizer;
grant execute on procedure sys.setoptimizer(string) to public;

create procedure sys.setquerytimeout("query" int)
	external name clients.setquerytimeout;
grant execute on procedure sys.setquerytimeout(int) to public;

create procedure sys.setsessiontimeout("timeout" int)
	external name clients.setsessiontimeout;
grant execute on procedure sys.setsessiontimeout(int) to public;

create procedure sys.setworkerlimit("limit" int)
	external name clients.setworkerlimit;
grant execute on procedure sys.setworkerlimit(int) to public;

create procedure sys.setmemorylimit("limit" int)
	external name clients.setmemorylimit;
grant execute on procedure sys.setmemorylimit(int) to public;

-- The super user can change the properties of all sessions
create procedure sys.setoptimizer("sessionid" int, "optimizer" string)
	external name clients.setoptimizer;

create procedure sys.setquerytimeout("sessionid" int, "query" int)
	external name clients.setquerytimeout;

create procedure sys.setsessiontimeout("sessionid" int, "query" int)
	external name clients.setsessiontimeout;

create procedure sys.setworkerlimit("sessionid" int, "limit" int)
	external name clients.setworkerlimit;

create procedure sys.setmemorylimit("sessionid" int, "limit" int)
	external name clients.setmemorylimit;

create procedure sys.stopsession("sessionid" int)
	external name clients.stopsession;

create procedure sys.setprinttimeout("timeout" integer)
	external name clients.setprinttimeout;

-- session's prepared statements
create function sys.prepared_statements()
returns table(
	"sessionid" int,
	"username" string,
	"statementid" int,
	"statement" string,
	"created" timestamp
)
external name sql.prepared_statements;
grant execute on function sys.prepared_statements to public;

create view sys.prepared_statements as select * from sys.prepared_statements();
grant select on sys.prepared_statements to public;

-- session's prepared statements arguments
create function sys.prepared_statements_args()
returns table(
	"statementid" int,
	"type" string,
	"type_digits" int,
	"type_scale" int,
	"inout" tinyint,
	"number" int,
	"schema" string,
	"table" string,
	"column" string
)
external name sql.prepared_statements_args;
grant execute on function sys.prepared_statements_args to public;

create view sys.prepared_statements_args as select * from sys.prepared_statements_args();
grant select on sys.prepared_statements_args to public;

create function sys.current_sessionid() returns int
external name clients.current_sessionid;
grant execute on function sys.current_sessionid to public;
