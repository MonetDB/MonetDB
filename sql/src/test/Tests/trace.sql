set optimizer='default_pipe';	-- to avoid different answers
set trace = 'none'; -- non-documented feature to not get any trace output

create function tracelog() 
	returns table (
		event integer,		-- event counter
		clk varchar(20), 	-- wallclock, no mtime in kernel
		pc varchar(50), 	-- module.function[nr]
		thread int, 		-- thread identifier
		ticks integer, 		-- time in microseconds
		reads integer, 		-- number of blocks read
		writes integer, 	-- number of blocks written
		rbytes integer,		-- amount of bytes touched
		wbytes integer,		-- amount of bytes written
		type string,		-- return types
		stmt string			-- actual statement executed
	)
	external name sql.dump_trace;

TRACE SELECT count(*) FROM types;
SELECT COUNT(*) FROM tracelog();
