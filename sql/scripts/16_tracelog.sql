-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

-- make the offline tracing table available for inspection
create function sys.tracelog()
	returns table (
		event integer,		-- event counter
		clk varchar(20),	-- wallclock, no mtime in kernel
		pc varchar(50),	-- module.function[nr]
		thread int,		-- thread identifier
		ticks bigint,		-- time in microseconds
		rrsMB bigint,		-- resident memory in MB
		vmMB bigint,		-- virtual size in MB
		reads bigint,		-- number of blocks read
		writes bigint,		-- number of blocks written
		minflt bigint,		-- minor page faults
		majflt bigint,		-- major page faults
		nvcsw bigint,		-- non-volantary conext switch
		stmt string		-- actual statement executed
	)
	external name sql.dump_trace;

create view sys.tracelog as select * from sys.tracelog();
