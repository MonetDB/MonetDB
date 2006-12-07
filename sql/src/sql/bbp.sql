create function bbp () 
	returns table (id int, name string, htype int, ttype int, count BIGINT, refcnt int, lrefcnt int, location string, heat int, dirty string, status string, kind string) 
	external name sql.bbp;
