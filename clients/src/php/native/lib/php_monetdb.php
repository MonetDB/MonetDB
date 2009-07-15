<?php
	/*
	Implementation of the driver API.
	
	Follows the specifications of the previous Cimpl: http://monetdb.cwi.nl/SQL/Documentation/The-PHP-Library.html
	
	- function monetdb_connect() 
	- function monetdb_disconnect() 
    - function monetdb_connected() 
	- function monetdb_query($query) 
	- function monetdb_fetch_assoc($resource) 
	- function monetdb_fetch_object($resource) 
	- function monetdb_num_rows($resource) 
	- function monetdb_affected_rows($resource) 
	- function monetdb_last_error() 
	- function monetdb_insert_id($seq) 
	- function monetdb_quote_ident($str) 
	- function monetdb_escape_string($str) 


	*/

	require 'php_mapi.inc';

	
	function monetdb_connect() {
	 	$options["host"] = "127.0.0.1";
		$options["port"] = "50000";

		$options["username"] = "monetdb";
		$options["password"] = "monetdb";
		$options["hashfunc"] = "sha1";	
		$options["database"] = "ruby"; 
		
		mapi_connect_proxy($options);
	}


	function monetdb_disconnect() {
		mapi_close();
	}
	
	/* Return the operation performed */
	function monetdb_query($query) {
		$buf = mapi_write(format_query($query));
		mapi_query(mapi_read());
	}
	
	function format_query($query) {
		return "s" . $query . ";";
	}
	
	function monetdb_escape_string($str) {
		
	}

	if (monetdb_connect() == FALSE) {
		print "Connection failed\n";
	}
	
	monetdb_query("select * FROM tables");
	
	monetdb_disconnect();
		
?>