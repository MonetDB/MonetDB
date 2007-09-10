create function clients () 
	returns table (id int, name string, login string, lastcmd string,
		 actions int, seconds BIGINT) 
	external name sql.clients;
