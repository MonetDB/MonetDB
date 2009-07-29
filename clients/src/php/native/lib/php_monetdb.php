<?php
	/**
	* php_monetdb.php
	* Implementation of the driver API.
	*
	* This library relies on the native PHP mapi implementation.
	* 
	* This file should be included by all pages that want to make use of a
 	* database connection.
	*
	* Synopsis of the provided functions:
	*
	* function monetdb_connect() *
	* function monetdb_disconnect()  *
    * function monetdb_connected() *
	* function monetdb_query($query)  *
	* function monetdb_fetch_assoc($hdl) *
	* function monetdb_fetch_object($hdl) 
	* function monetdb_num_rows($hdl)  *
	* function monetdb_affected_rows($hdl) * 
	* function monetdb_last_error()  *
	* function monetdb_insert_id($seq)  
	* function monetdb_quote_ident($str) *
	* function monetdb_escape_string($str) * 
	**/

	/**
	* php_mapi.inc is a native (socket based) php implementation of the MAPI communication protocol.
	*/
	require 'php_mapi.inc';
	
	/**
	 * Opens a connection to a MonetDB server.  
	 * 
	 * @return bool TRUE on success or FALSE on failure 
	 */
	
	function monetdb_connect($host = "127.0.0.1", $port = "50000", $database = "ruby" , $username = "monetdb", $password = "monetdb" ) {
	 	$options["host"] = $host;
		$options["port"] = $port;

		$options["username"] = $username;
		$options["password"] = $password;
		$options["hashfunc"] = "sha1";	
		$options["database"] = $database; 
		
		return mapi_connect_proxy($options);
	}

	/**
	 * Disconnects the connection to the database.
	 *
	 * @param resource connection instance
	 */
	function monetdb_disconnect($conn=NULL) {
		mapi_close($conn);
	}
	
	/**
	 * Returns whether a connection to the database has been made, and has
	 * not been closed yet.  Note that this function doesn't guarantee that
	 * the connection is alive or usable.
	 *
	 * @param resource connection instance
	 * @return bool TRUE if there is a connection, FALSE otherwise
	 *
	 */
	function monetdb_connected($conn) {
		return mapi_connected($conn);
	}
	
	/**
	 * Executes the given query on the database.
	 *
	 * @param resource connection instance
	 * @param string the SQL query to execute
	 * @return resource a query handle or FALSE on failure
	 */
	function monetdb_query($connection=NULL, $query) {
		return mapi_execute($connection, $query);
	}
	
	
	/**
	 * Returns the number of rows in the query result.
	 *
	 * @param resouce the query resource
	 * @return int the number of rows in the result
	 */
	function monetdb_num_rows($hdl) {
		if ($hdl["operation"] == Q_TABLE || $hdl["operation"] == Q_BLOCK ) {
			return $hdl["query"]["rows"];
		} else {
			print "Last query did not produce a result set\n";
			return -1;
		}
	}
	
	/**
	 * Returns an array containing column values as value.
	 *
	 * @param resource the query handle
	 * @param int the position of the row to retrieve
	 * @return array the next row in the query result as associative array or
	 *         FALSE if no more rows exist
	 */
	function monetdb_fetch_row(&$hdl, $row=-1) {		
		
		if ($hdl["operation"] != 1) {
			return FALSE;
		}
		
		if ($row == -1){
			$row = $hdl["last_row"];
		} else {
			$row -= 1; // ith row in the database is store at position i-1 in the array.
		}	
	
		
		if ($row > $hdl["query"]["rows"]) {
			// print "Error: the requested index exceeds the number of rows \n";
			return FALSE;
		}
	
		$hdl["last_row"] += 1;

		return $hdl["record_set"][$row];
	}
	
	/**
	 * Returns an associative array containing the column names as keys, and
	 * column values as value.
	 *
	 * @param resource the query handle
	 * @param int the position of the row to retrieve
	 * @return array the next row in the query result as associative array or
	 *         FALSE if no more rows exist
	 */	
	function monetdb_fetch_assoc(&$hdl, $row=-1) {
		global $last_row;
		
		if ($hdl["operation"] != Q_TABLE) {
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



	/**
	 * Returns the result in the given query resource as object one row at a time.  Column
	 * names become members of the object through which the column values
	 * can be retrieved.
	 *
	 * @param resource the query handle
	 * @param int the position of the row to retrieve
	 * @return the query result as object or FALSE if there are no more rows
	 */
	function monetdb_fetch_object(&$hdl, $row=-1)  {	
		$row_array =  monetdb_fetch_assoc(&$hdl, $row);
		$row_object = new stdClass();
		
		if (is_array($row_array) && count($row_array) > 0) {
			foreach ($row_array as $name=>$value) {
		   		$name = strtolower(trim($name));
		        if (!empty($name)) {
		        	$row_object->$name = $value;
		        }
		     }
		   }
		return $row_object;
	}
	
	/**
	 * Returns the number of affected rows for an UPDATE, INSERT or DELETE
	 * query.  The number of affected rows typically is 1 for INSERT
	 * queries.
	 *
	 * @param resource the query resource
	 * @return int the number of affected rows
	 */	
	function monetdb_affected_rows($hdl) {
		if ($hdl["operation"] != Q_UPDATE) {
			return 0;
		} 
		
		return $hdl["query"]["affected"];
	}
	
	/**
	 * Returns the last error reported by the database.
	 *
	 * @return string string the last error
	 */
	function monetdb_last_error() {
		global $last_error;
		return $last_error;
	}
	
	/**
	* TODO
	*/
	function monetdb_insert_id($seq)  {
		return FALSE;
	}
	
	/**
	 * Returns a 'quoted identifier' suitable for MonetDB.
	 * This utility function can be used in queries to for instance quote
	 * names of tables of columns that otherwise would be a mistaken for a
	 * keyword.
	 * NOTE: the given string is currently not checked for validity, hence
	 *       the output of this function may be an invalid identifier.
	 *
	 * @param string the identifier to quote
	 * @return string the quoted identifier
	 */
	function monetdb_quote_ident($str) {
		return('"'. $str .'"');
	}

	/**
	 * Returns an 'escaped' string that can be used for instance within
	 * single quotes to represent a CHARACTER VARYING object in SQL.
	 *
	 * @param string the string to escape
	 * @return string the escaped string
	 */
	function monetdb_escape_string($str) {
		// NB: temporary solution; I'm studying better way to deal with possible sql injections issues.
		return addslashes($str);
	}
	
	
	
	
		
	/* These functions are not present in the original Cimpl implementation
	 * TODO: make it connection aware
	 */
	function create_savepoint(&$conn) {
		if ($conn != NULL) {
			$index = count($conn["transactions"]);
		
			$id = "monetdbsp" . $index;
			array_push($conn["transactions"], $id);
			
			return TRUE;
		}
		
		return FALSE;
	}
	
	function release_savepoint(&$conn) {
		if ($conn != NULL) { 
			return array_pop($conn["transactions"]);
		}
		
		return FALSE;
	}
	
	function get_savepoint(&$conn) {
		if (count($conn["transactions"]) == 0) {
			return FALSE;
		}
		
		// return the last element in the array
		return $conn["transactions"][count($conn["transactions"])-1];
	}
	
	// turn auto commit on/off.
	function auto_commit($conn, $flag=TRUE) {
		if ($conn["socket"] != NULL) {
			$cmd = "auto_commit " . $flag;
			write_command($socket, $cmd);
			
			return TRUE;
		}
		return FALSE;
	}
	
?>