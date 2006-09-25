
create function query_cache () 
	returns table (query string, count int) 
	external name sql.dump_cache;
