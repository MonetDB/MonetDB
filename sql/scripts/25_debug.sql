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
-- Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
-- Copyright August 2008-2014 MonetDB B.V.
-- All Rights Reserved.

-- show the optimizer statistics maintained by the SQL frontend
create function sys.optimizer_stats () 
	returns table (rewrite string, count int) 
	external name sql.dump_opt_stats;


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
create function sys.environment()
	returns table ("name" string, value string)
	external name sql.sql_environment;
create view sys.environment as select * from sys.environment();

update sys._tables
    set system = true
    where name in ( 'environment', 'optimizers')
        and schema_id = (select id from sys.schemas where name = 'sys');

-- The BAT buffer pool overview
create function sys.bbp () 
	returns table (id int, name string, htype string, 
		ttype string, count BIGINT, refcnt int, lrefcnt int, 
		location string, heat int, dirty string, 
		status string, kind string) 
	external name bbp.get;

create procedure sys.evalAlgebra( ra_stmt string, opt bool)
	external name sql."evalAlgebra";
