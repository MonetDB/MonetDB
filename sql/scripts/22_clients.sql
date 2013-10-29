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

create function sys.password_hash (username string) 
	returns string 
	external name sql.password;

create function sys.sessions()
returns table("user" string, "login" timestamp, "sessiontimeout" bigint, "lastcommand" timestamp, "querytimeout" bigint, "active" bool)
external name sql.sessions;
create view sys.sessions as select * from sys.sessions();
update sys._tables
    set system = true
    where name = 'sessions'
        and schema_id = (select id from sys.schemas where name = 'sys');

create procedure sys.shutdown(delay tinyint) 
external name sql.shutdown;

create procedure sys.shutdown(delay tinyint, force bool) 
external name sql.shutdown;

-- control the query and session time out 
create procedure sys.settimeout("query" bigint)
	external name sql.settimeout;
create procedure sys.settimeout("query" bigint, "session" bigint)
	external name sql.settimeout;
create procedure sys.setsession("timeout" bigint)
	external name sql.setsession;
