
create function optimizer_stats () 
	returns table (rewrite string, count int) 
	external name sql.dump_opt_stats;
