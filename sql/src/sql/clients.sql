create function clients () 
	returns table ("user" string, login string, lastcommand string,
		 actions int, seconds BIGINT) 
	external name sql.clients;
