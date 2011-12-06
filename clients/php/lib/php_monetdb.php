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
	* function monetdb_insert_id($seq)  *
	* function monetdb_quote_ident($str) *
	* function monetdb_escape_string($str) * 
	**/

	/**
	* php_mapi.inc is a native (socket based) php implementation of the MAPI communication protocol.
	*/
	require 'php_mapi.inc';
	
	/**
	* register a 'monetdb' extension to retain compatibility with wht Cimpl based scripts.
	*/
	
	/**
	 * Opens a connection to a MonetDB server.  
	 * 
	 * @param string language to be used (sql)
	 * @param string hostname to connect to (default is localhost)
	 * @param int    port to use (default is 50000)
	 * @param string username (default is monetdb)
	 * @param string password (default is monetdb)
	 * @param string database to use (default is demo)
	 * @param string hash function to use during authentication (defaults to SHA1) 
	 * @return bool TRUE on success or FALSE on failure 
	 */
	
	function monetdb_connect($lang = "sql", $host = "127.0.0.1", $port = 50000, $username = "monetdb", $password = "monetdb", $database = "demo", $hashfunc = "") {
	 	$options["host"] = $host;
		$options["port"] = $port;

		$options["username"] = $username;
		$options["password"] = $password;
		
		$options["database"] = $database; 
		$options["persistent"] = FALSE;
	
	
	    if ($hashfunc == "") {
		    $hashfunc = "sha1";
	    }
	    
	    if ($lang == "") {
	        $lang = "sql";
	    }
	    
	    $options["hashfunc"] = $hashfunc;
        $options["lang"]     = $lang;
				
		return mapi_connect_proxy($options);
	}

	/**
	 * Opens a persistent connection to a MonetDB server.  
	 * First, when connecting, the function would first try to find a (persistent) link that's already open with the same host, 
	 * username and password. If one is found, an identifier for it will be returned instead of opening a new connection.
	 *
	 * Second, the connection to the SQL server will not be closed when the execution of the script ends. 
	 * Instead, the link will remain open for future use (monetdb_close() will not close links established by monetdb_pconnect()).
	 *
	 * This type of link is therefore called 'persistent'. 
	 *
	 * @param string language to be used (sql)
	 * @param string hostname to connect to (default is localhost)
	 * @param int    port to use (default is 50000)
	 * @param string username (default is monetdb)
	 * @param string password (default is monetdb)
	 * @param string database to use (default is demo)
	 * @param string hash function to use during authentication (defaults to SHA1)
	 * @return bool TRUE on success or FALSE on failure 
	 */
	
  function monetdb_pconnect($lang = "sql", $host = "127.0.0.1", $port = 500000, $username = "monetdb", $password = "monetdb", $database = "demo", $hashfunc = "") {

	 	$options["host"] = $host;
		$options["port"] = $port;

		$options["username"] = $username;
		$options["password"] = $password;
		$options["database"] = $database; 
		$options["persistent"] = TRUE;
		
		if ($hashfunc == "") {
		    $hashfunc = "sha1";
	    }
	    
	    if ($lang == "") {
	        $lang = "sql";
		} else if (strstr($lang, "sql") == $lang) {
			$lang = "sql";
		}
	    
	    $options["hashfunc"] = $hashfunc;
        $options["lang"]     = $lang;
		
		return mapi_connect_proxy($options);
	}

	/**
	 * Disconnects the connection to the database.
	 *
	 * @param resource connection instance
	 */
	function monetdb_disconnect($conn=NULL) {
		$num_args = func_num_args();
		
		if ($num_args == 0) {
			$conn = mapi_get_current_conn();
			mapi_close(NULL);
		} else {
			mapi_close($conn);
		}
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
	function monetdb_connected($connection=NULL) {
		$num_args = func_num_args();
		
		if ($num_args == 0){
			return mapi_connected(mapi_get_current_conn());
		}
		
		return mapi_connected($connection);
	}
	
	/**
	 * Executes the given query on the database.
	 *
	 * @param resource connection instance
	 * @param string the SQL query to execute
	 * @return resource a query handle or FALSE on failure
	 */
	function monetdb_query($connection=NULL, $query="") {
		$num_args = func_num_args();
		
		if ($num_args == 1){
			$arg = func_get_arg(0);
			if (is_string($arg)) {
				$conn = mapi_get_current_conn();
				if ($conn != NULL) {
					return mapi_execute($conn, $arg);
				} 
			}
			
		} else {
			return mapi_execute($connection, $query);
		}
		
		return FALSE;
	}
	
	/**
	 * Returns the number of rows in the query result.
	 *
	 * @param resouce the query resource
	 * @return int the number of rows in the result; FALSE if the query did not return any result set
	 */
	function monetdb_num_rows($hdl) {
		if ($hdl["operation"] == Q_TABLE || $hdl["operation"] == Q_BLOCK ) {
			return $hdl["query"]["rows"];
		} else {
			return FALSE;
		}
	}

	/**
	 * Returns the number of fields in the query result.
	 *
	 * @param resouce the query resource
	 * @return int the number of fields in the result; FALSE if the query did not return any result set
	 */	
	function monetdb_num_fields($hdl) {
    	if ($hdl["operation"] == Q_TABLE || $hdl["operation"] == Q_BLOCK ) {
  			return $hdl["query"]["fields"];
  		} else {
  			return FALSE;
  		}
	}
	
	/**
	 * Returns an array containing column values as value. 
	 * For efficiency reasons the array pointer is not reset when calling monetdb_fetch_row
	 * specifying a row value. 
	 *
	 * @param resource the query handle
	 * @param int the position of the row to retrieve
	 * @return array the next row in the query result as associative array or
	 *         FALSE if no more rows exist
	 */
	function monetdb_fetch_row(&$hdl, $row=-1) {	
		global $last_error;
		
		if ($hdl["operation"] != Q_TABLE && $hdl["operation"] != Q_BLOCK ) {
			return FALSE;
		}
	
		if ($row == -1){
		  // Parse the tuple and present it to the user
			$entry = current($hdl["record_set"]);
			
			//advance the array of one position
			next($hdl["record_set"]);

		} else {
			if ($row < $hdl["query"]["rows"]) {
			  /* Parse the tuple and present it to the user*/
				$entry = $hdl["record_set"][$row-1];
			}
			else {
				$last_error = "Index out of bound\n";
				return FALSE;
			}
		}	
		
    if ($entry) {
		  return php_parse_row($entry);
	  }
	  
	  return $entry;
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
		if ($hdl["operation"] != Q_TABLE && $hdl["operation"] != Q_BLOCK ) {
			return FALSE;
		}
		
		// first retrieve the row as an array
		$fetched_row =  monetdb_fetch_row($hdl, $row);
		
		if ($fetched_row == FALSE) {
			return FALSE;
		}
		
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
		
		if ($hdl["operation"] != Q_TABLE && $hdl["operation"] != Q_BLOCK ) {
			return FALSE;
		}
		
		if (($row_array =  monetdb_fetch_assoc(&$hdl, $row)) == FALSE) {
			return FALSE;
		}
		
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
	* Returns the name of the field at position $field
	*
	* @param resource the query handle
	* @param int field number
	* @return string the field name, FALSE is an error occured.
	*/
	function monetdb_field_name(&$hdl, $field) {
	    if (is_array($hdl) && $field >= 0) {
            if ($hdl["operation"] == Q_TABLE || $hdl["operation"] == Q_BLOCK ) {
                if ($hdl["header"]["fields"] != "" ) {
                    return $hdl["header"]["fields"][$field];
                }
		    }
    	}	
    	return FALSE;
	}
	
	/**
	 * Returns the number of affected rows for an UPDATE, INSERT or DELETE
	 * query.  The number of affected rows typically is 1 for INSERT
	 * queries.
	 *
	 * @param resource the query handle
	 * @return int the number of affected rows, FALSE if the last executed query did not affect any row.
	 */	
	function monetdb_affected_rows($hdl) {
		if ($hdl["operation"] != Q_UPDATE) {
			return FALSE;
		} 
		
		return $hdl["query"]["affected"];
	}
	
	
	/**
	* Check if the query handle contains a result set. Note: the result set may be empty.
	*
	* @param resource the query handle
	* @return bool TRUE if the query contains a result set, FALSE otherwise.
	*/
	function monetdb_has_result($hdl) {
	    if (($hdl["operation"] == Q_TABLE) || ($hdl["operation"] == Q_BLOCK)) {
	        return TRUE;
	    }
	    
	    return FALSE;
	}
	
	/**
	 * Returns the last error reported by the database.
	 *
	 * @return string the last error reported
	 */
	function monetdb_last_error() {
		global $last_error;
		return $last_error;
	}
	
	/**
	 * Generates the next id in the sequence $seq
	 *
	 * @param resource connection instance
	 * @param seq sequence whose next
	 * value we want to retrieve
	 * @return string the ID of the last tuple inserted. FALSE if an error occurs
	 */
	function monetdb_insert_id($connection = NULL, $seq)  {
		$num_args = func_num_args();
		
		if ($num_args == 1) {
			$connection = mapi_get_current_conn();
			$seq = func_get_arg(0);
		}
		
		if (is_string($seq)) {
			$query = "SELECT NEXT VALUE FOR ".monetdb_quote_ident($seq)."";
			$res = monetdb_query($connection, $query);
			$row = monetdb_fetch_assoc($result);
            return($row[$seq]);
		}
		
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
	 *
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
		return mapi_quote($str, strlen($str));
	}
	
	/**
	* Free the result set memory.
	* 
	* @param resource the query handle.
	* @return bool returns TRUE on success or FALSE on failure.
	*
	*/
	function monetdb_free_result(&$hdl) {
		$conn_id = $hdl["conn"];
		$res_id = $hdl["query"]["id"];

		/* Release the result set on server */
		mapi_free_result($conn_id, $res_id);
		
		
		if (isset($hdl) && is_array($hdl)) {
			foreach($hdl as $field) {
				if (isset($field)) {
					unset($field);
				}
			}
			
			unset($hdl);
			
			return TRUE;
		}
		
		return FALSE;
	}
		
	
	/* 
	 * These functions are not present in the original Cimpl implementation
	 */
	
	/**
	* Create a new savepoint ID
	* @param resource connection instance
	* @return bool TRUE if the ID has been correctly generated, FALSE otherwise.
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
	
	/**
	* Release a savepoint ID.
	* @param resource connection instance
	* @return bool TRUE if the ID has been correctly released, FALSE otherwise.
	*/
	function release_savepoint(&$conn) {
		if ($conn != NULL) { 
			array_pop($conn["transactions"]);
			return TRUE;
		}
		
		return FALSE;
	}

	/**
	* Return the current (last generated) savepoint ID.
	* @param resource connection instance
	* @return string savepoint ID. I no savepoints are available, FALSE is returned.
	*/
	function get_savepoint(&$conn) {
		if (count($conn["transactions"]) == 0) {
			return FALSE;
		}
		
		// return the last element in the array
		return $conn["transactions"][count($conn["transactions"])-1];
	}

	/**
	* Sets auto commit mode on/off
	* @param resource connection instance
	* @param bool TRUE to turn auto commit on, FALSE to turn it off.
	* @return bool TRUE is auto commit mode was correctly set. FALSE otherwise.
	*/
	function auto_commit($conn, $flag=TRUE) {
		if ($conn["socket"] != NULL) {
			$cmd = "auto_commit " . $flag;
			mapi_write($conn["socket"], format_command($cmd));
			
			return TRUE;
		}
		return FALSE;
	}
	
?>
