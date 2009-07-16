<?php
	/*
	Implementation of the driver API.
	
	- function monetdb_connect() *
	- function monetdb_disconnect()  *
    - function monetdb_connected() *
	- function monetdb_query($query)  *
	- function monetdb_fetch_assoc($resource) *
	- function monetdb_fetch_object($resource) 
	- function monetdb_num_rows($resource)  *
	- function monetdb_affected_rows($resource) * 
	- function monetdb_last_error()  *
	- function monetdb_insert_id($seq)  
	- function monetdb_quote_ident($str) *
	- function monetdb_escape_string($str) * 


	*/

	require 'php_mapi.inc';

	/* Use a global function to index the last returned row from a record set.
	   I do this instead of passing the result set handler by reference to the proper functions
	   in order to retain syntax compabilitiy with the previous API.
	*/
	$last_row = 0; 
	
	
	function monetdb_connect() {
	 	$options["host"] = "127.0.0.1";
		$options["port"] = "50000";

		$options["username"] = "monetdb";
		$options["password"] = "monetdb";
		$options["hashfunc"] = "sha1";	
		$options["database"] = "ruby"; 
		
		return mapi_connect_proxy($options);
	}


	function monetdb_disconnect() {
		mapi_close();
	}
	
	function monetdb_connected() {
		return mapi_connected();
	}
	
	function monetdb_num_rows($handle) {
		if ($handle["operation"] == Q_TABLE || $handle["operation"] == Q_BLOCK ) {
			return $hanlde["query"]["rows"];
		} else {
			print "Last query did not produce a result set\n";
			return -1;
		}
	}
	
	function monetdb_fetch_row($hdl, $row=-1) {
		global $last_row;
		
		if ($hdl["operation"] != QTABLE) {
			return FALSE;
		}
		
		if ($row == -1){
			$row = $last_row;
		}	
		
		if ($row < $hdl["rows"]) {
			print "Error: the requested index exceeds the number of rows \n";
			return FALSE;
		}
		
		$last_row++;
		return $hdl["record_set"][$row];
	}
	
	function monetdb_fetch_assoc($hdl, $row=-1) {
		global $last_row;
		
		if ($hdl["operation"] != QTABLE) {
			return FALSE;
		}
		
		// first retrieve the row as an array
		$fetched_row =  monetdb_fetch_row($hdl, $row);
		
		// now hash the array by field name		
		$hashed = array();

		$i = 0;
		foreach ($hdl["header"]["fields"] as $field) {
			$field = str_replace(" ", "", $field);
			$hashed[$field] = $fetched_row[$i];
			$i++;
		}
		
		return $hashed;
	}
	
	function monetdb_affected_rows($hdl) {
		if ($hdl["operation"] != Q_UPDATE) {
			return 0;
		} 
		
		return $hdl["query"]["affected"];
	}
	
	/* Return the operation performed */
	function monetdb_query($query) {
		$buf = mapi_write(format_query($query));
		return mapi_query(mapi_read());
	}
	
	function format_query($query) {
		return "s" . $query . ";";
	}
	
	function monetdb_quote_dent($str) {
		return('"'.$str.'"');
	}
	
	function monetdb_escape_string($str) {
		// NB: temporary solution; I'm studying better way to deal with possible sql injections issues.
		return addslashes($str);
	}

	
	if (monetdb_connect() == FALSE) {
		print "Connection failed\n";
	}
	
	$hdl = monetdb_query("INSERT into test VALUES (1)");
	
   	print "Affected " . monetdb_affected_rows($hdl) . "\n"; 
	
	monetdb_disconnect();
		
?>