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
-- Copyright August 2008-2011 MonetDB B.V.
-- All Rights Reserved.

-- make the offline tracing table available for inspection
create function tracelog() 
	returns table (
		event integer,		-- event counter
		clk varchar(20), 	-- wallclock, no mtime in kernel
		pc varchar(50), 	-- module.function[nr]
		thread int, 		-- thread identified
		ticks integer, 		-- time in microseconds
		reads integer, 		-- number of blocks read
		writes integer, 	-- number of blocks written
		rbytes integer,		-- amount of bytes touched
		wbytes integer,		-- amount of bytes written
		type string,		-- return types
		stmt string			-- actual statement executed
	)
	external name sql.dump_trace;

