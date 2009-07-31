<?php
/**
   *	Implementation of the MAPI protocol (v9).
   *
   *
   * 	Provides: 
   * -  mapi_query($data) {
   * -  mapi_store($data) {
   * -  php_parse_tuples($rows) {
   * -  mapi_connect() {
   * -  mapi_authenticate($user, $passwd, $hash, $salt, $dbname) {
   * -  mapi_read() {
   * -  mapi_write($msg) {
   * -  set_timezone() {
   * -  format_command($cmd) {
   * -  mapi_connect_proxy() {
   * -  mapi_open() {
   * - mapi_close() {
   *
**/
	
	define("MAX_PACKET_SIZE", 32767); // Maximum packet size 
	
	define("REPLY_SIZE", "250"); // Set the initial reply size
	
	define("Q_TABLE", "1"); // SELECT operation
	define("Q_UPDATE", "2"); // INSERT/UPDATE operations
	define("Q_CREATE", "3"); // CREATE/DROP TABLE operations
	define("Q_TRANSACTION", "4"); // TRANSACTION
	define("Q_PREPARE", "5"); 
	define("Q_BLOCK", "6"); // QBLOCK message

	define("MSG_REDIRECT", "^"); // auth redirection through merovingian
	define("MSG_QUERY", "&");
	define("MSG_SCHEMA_HEADER", "%");
	define("MSG_INFO", "!"); // info response from mserver
	define("MSG_TUPLE", "[");
	define("MSG_PROMPT", "");

	define("PROTOCOL_v8", 8); // supported protocol(s)	
	define("PROTOCOL_v9", 9); // supported protocol(s)
	
	define('MAX_MEROVINGIAN_ITER', 10); // Maximum number of iterations during proxied auth through merovingian

	define('TIMEZONE', 'Europe/Amsterdam'); // Arbitrarly defined timezone.

//	if (date_default_timezone_get() == "") {
	if (ini_get('date.timezone') == "") {
		/*
		* PHP was not able to determine the timezone on the machine. Default to a pre set and warn the user.
		*/
		date_default_timezone_set(TIMEZONE); /* Set the default timezone. */
		trigger_error("PHP was not able to determine the timezone on the machine. The default will be " . TIMEZONE, E_USER_NOTICE);
	}
	

	/**
	* $connection_pool contains a list of the active database connections
	*/
	$connection_pool = array();
	
	/**
	* Stores the last error reported by the server
	*/
	$last_error = "";	
	
	/**
	* Execute a query and return the resulting handle by reference.
	*/
	function &mapi_execute($conn=NULL, $query) {
		global $connection_pool;
			
		
		/**
		* Query handle
		* 
		* "conn" - id of the connection that fired the query
		* "header" - table header
		* "record_set" - retrieved record set (if present)
		* "operation" - query type
		*
		*/
		$handle = array("conn" => "", "header" => array(), "query" => "", "record_set" => array(), "operation" => -1, "last_row" => 0);
		
		// if multiple connections are present, require the user to specify which one to use.
		if ($conn == NULL) {
			return FALSE;
		} else if (($socket = $conn["socket"]) == NULL) {
			return FALSE;
		}
		
		$handle["conn"] = $conn["id"];		
		
		/* Fire the query and read back the response */
		$buf = mapi_write($socket, format_query($query));
		
		
		$data = mapi_read($socket);
		
		if ( ($operation = mapi_store($data, $handle)) == FALSE) {
			return FALSE;
		}
		
		/* The query produced a record set */
		if ($operation == Q_TABLE || $operation == Q_BLOCK) {
			// fetch the whole result set
			if ($handle["query"]["index"] < $handle["query"]["rows"]) {
				mapi_fetch_next($socket, $handle);
			}
		}
			
		if ($handle == NULL) {
			return FALSE;
		}
		return $handle;
	}
	
	function mapi_fetch_next($socket, &$handle){
		if ($socket == NULL){
			return FALSE;
		}
		
		while ($handle["query"]["index"] < $handle["query"]["rows"]) {
			// export a new window
			$left_rows = $handle["query"]["rows"] - $handle["query"]["index"];
				
			$exp_size = min(10, $left_rows);
			mapi_write($socket, format_command("export " . $handle["query"]["id"] . " " . $handle["query"]["index"] . " " . $exp_size));				
			$data = mapi_read($socket);
				
			if ( ($operation = mapi_store($data, $handle)) == FALSE ) {
				return FALSE;
			}
				
			$handle["query"]["index"] += $exp_size;
			$handle["operation"] = $operation;
		}
	
		return $handle;		
	}
	
	function mapi_store($data, &$handle) {	
		global $last_error;
					
		$data = explode("\n", $data);
		
		$record_set = "";
		$operation = "";
				
		$header = ""; // store temporary header informations;
		
		$rows = ""; // stores (partially) retrieved rows
		foreach ($data as $row) {			
		//	print $row;
			if ($row[0] == MSG_QUERY) {
				if ($row[1] == Q_TABLE) {
					$operation = Q_TABLE;
					// save info about the query
					$fields = split(" ", $row);
					$handle["query"] = array("id" => $fields[1], "rows" => $fields[2], "cols" => $fields[3], "index" => $fields[4]);
					
				} else if ($row[1] == Q_UPDATE) {
					$operation = Q_UPDATE;
					$fields = split(" ", $row);
					$handle["query"] = array("affected" => $fields[1]);					
				} else if ($row[1] == Q_CREATE) {
					$operation = Q_CREATE;
				} else if ($row[1] == Q_TRANSACTION) {
					$operation = Q_TRANSACTION;
				} else if ($row[1] == Q_BLOCK) {
					// add Q_BLOCK to the record set
					$operation = Q_BLOCK;
				}
			} else if ($row[0] == MSG_SCHEMA_HEADER){
				// process the table header
				$header = $header . $row . "\n";
			} else if ($row[0] == MSG_TUPLE) {
				// process tuples
				$rows = $rows . $row;
			} else if ($row[0] == MSG_PROMPT) {
				// receive all the tuples
				$record_set = php_parse_tuples($rows);
				$rows = "";
			} else if ($row[0] == MSG_INFO) {
				$last_error = $row;
				return FALSE;
			}
		}
		
		if ($record_set != "") {
			$handle["record_set"] = array_merge($handle["record_set"], $record_set);
		}
		
		if ($header != "") {
			
			$handle["header"] = php_parse_header($header);
		}
		
		$handle["operation"] = $operation;
	
		return $operation;
	}
	
	
	function php_parse_tuples($rows) {
		$parsed_rows = "";
	
		$rows = rtrim($rows, "\t]");	
		$rows = split("\t]", $rows);
		foreach ($rows as &$row) {				
			
			$row = ltrim($row, "[");
			$row = split("\t",  $row);
			
			foreach ($row as &$field) {
				/* strip left/right \" chars and right ',' */
				$field = rtrim($field, ",");
				$field = rtrim($field, "\"");
				$field = ltrim($field, "\"");
			}
			$parsed_rows[] = $row;
		}
	
		return $parsed_rows;
	}
	
	function php_parse_header($header) {
		$header = explode("\n", $header);
		$name = $header[0];
		
		$header_array = array();
		
		/* Field names */
		$header[1] = ltrim($header[1], "%");
		$header[1] = split("#", $header[1]);
		$header_array["fields"] = explode(",\t", $header[1][0]);

		/* Field types */
		$header[2] = ltrim($header[2], "%");
		$header[2] = split("#", $header[2]);
		$header_array["types"] = explode(",\t", $header[2][0]);
		
		return $header_array;
		
	}

	/*
		$conn_opts is an array containing:
		username => 
		password =>
		host 	 =>
		port 	 =>
		database =>
		hash	 =>
	
	*/
	function mapi_connect($socket, $options, $merovingian_iter) {
		global $last_error;
		global $connection_pool;
		
		$host 	= $options["host"];
		$port 	= $options["port"];

		$user 	= $options["username"];
		$passwd = $options["password"];
		$hash 	= $options["hashfunc"];
		$dbname = $options["database"];
		
		# get server challenge
		$challenge = mapi_read($socket);				

		/*
			Array
			(
			    [0] => void
			    [1] => merovingian
			    [2] => 8
			    [3] => plain
			    [4] => LIT
			)
		
		*/
		$credentials = split(":", $challenge);
		
		// $challenge[0] contains the salt
		
		if ($credentials[2] == PROTOCOL_v8 ){
			// Protocol v8 supported only for legacy/testing reasons
			mapi_authenticate_v8($socket, $user, $passwd, $hash, $credentials[0], $dbname);
		} else if ($credentials[2] == PROTOCOL_v9) {
			// $credentials[5] contains pwhash
			mapi_authenticate_v9($socket, $user, $passwd, $hash, $credentials[0], $dbname, $credentials[5]);			
		} else {
			$last_error = "Protocol " . $credentials[2] . " not supported. Aborting.";
			return FALSE;
			
		}
		
		$response = mapi_read($socket);
		
		if ($response != MSG_PROMPT) { 
		// not ready to authenticate yet
			if ($response[0] == MSG_REDIRECT) {
				
				if ($credentials[1] == "merovingian") {
					if ($merovingian_iter < MAX_MEROVINGIAN_ITER) {
						$merovingian_iter++;
						mapi_connect($socket, $options, $merovingian_iter);
					}
				
				}
				else if ($credentials[1] == "mserver") {
					mapi_close($socket);
					mapi_connect(mapi_open(), $options);
				}
				
			} else if ($response[0] == MSG_INFO) {
				$last_error = $response;
				return FALSE;
			}
		}
		
		return TRUE;
	}
	
		
	function mapi_authenticate_v8($socket, $user, $passwd, $hash, $salt, $dbname) {
		$auth_string = "";
		
		if (strtolower($hash) != "plain") {
			$hashsum = hash(strtolower($hash), $passwd . $salt);
		} else {
			$hashsum = $passwd+$salt;
		}
		
		$auth_string = "BIG:" . $user . ":{" . strtoupper($hash) . "}" . $hashsum . ":sql:" . $dbname . ":";
		
		mapi_write($socket, $auth_string);
	}

	function mapi_authenticate_v9($socket, $user, $passwd, $hash, $salt, $dbname, $pwhash) {
		$auth_string = "";
	
		$pwhashsum = hash(strtolower($pwhash), $passwd);
		$hashsum   = hash(strtolower($hash), $pwhashsum . $salt);
		
		$auth_string = "BIG:" . $user . ":{" . strtoupper($hash) . "}" . $hashsum . ":sql:" . $dbname . ":";
		
		mapi_write($socket, $auth_string);
	}

	// decode the header and get the requested amount of data
	function mapi_read($socket=NULL) {
		# get the first 2 bytes
		$header = socket_read($socket, 2);
		$data = "";
		$chunk_size = "";

		$chunk_size = ((ord($header[1]) << 7) | (ord($header[0]) >> 1));
		$data = socket_read($socket, $chunk_size);
		
		
		while ((ord($header[0]) & 1) == 0 ) {
			$header = socket_read($socket, 2);
			$chunk_size = ( ((ord($header[1]))  << 7) | (ord($header[0]) >> 1) );
			
			$data = $data . socket_read($socket, $chunk_size);
		}
		if (strlen($data) == 0) {
			return "";
		}
		
		return $data;
	}


	// encode data and send it to the server. Returns the number of bytes sent.
	function mapi_write($socket=NULL, $msg) {
		global $last_error;
		
		$fb = 0;
		$sb = 0;
		
		$pos = 0;
		$data = "";
		
		$buf = 0;
		
		$is_final = FALSE;
		while (! $is_final) {						
			$data  = substr($msg, $pos, min(MAX_PACKET_SIZE, strlen($msg) - $pos)  );
			$pos += strlen($data);
								
			$end = 0; // more packets will follow
			if ( (strlen($msg) - $pos) == 0) {
				$is_final = TRUE;
				$end = 1;
			}
				
			$fb = (strlen($data) << 1) | $end; 
			
			/**
			  * socket_write() does not guarantee all data to be transmitted. 
			  * Make sure that the buffer is flushed.
			*/
			if ( ($buf = socket_flush($socket, pack("v", $fb) . $data)) == FALSE) {
				$last_error = socket_strerror(socket_last_error());			
				return -1;
			}
			
		}
		return $buf;
	}
	
	function set_timezone($socket=NULL) {
		global $last_error;

		$tz_offset = "'" . date('P') . "'"; /* Difference to Greenwich time (GMT) with colon between hours and minutes */
				
		$query = "SET TIME ZONE INTERVAL " . $tz_offset . " HOUR TO MINUTE";
	
		$buf = mapi_write($socket, format_query($query));
					
		$response = mapi_read($socket);
		
		if ($response == "") { 
			return TRUE; 
		} else { 
			
			$last_error = $response; 
			return $response; 
		}
	}
	
	function format_command($cmd) {
		return "X" . $cmd . "\nX";
	}
	
	
	function mapi_connect_proxy($options) {
		global $last_error;
		global $connection_pool;
		
		$merovingian_iter = 0;
		
		$host 	= $options["host"];
		$port 	= $options["port"];

		$user 	= $options["username"];
		$passwd = $options["password"];
		$hash 	= $options["hashfunc"];
		$dbname = $options["database"];
		
		$socket	= mapi_open();

		if (socket_connect($socket, $host, $port) == FALSE) {
			$last_error = socket_strerror(socket_last_error());
			return FALSE;
		}
		
		if ( ($connected = mapi_connect($socket, $options, $merovingian_iter)) == TRUE ) {
			/* Connected */
			set_timezone($socket); // set the timezone according to the system's configuration
			
			// export the reply size (max number of tuples returned at query executions)
			mapi_write($socket, format_command("reply_size " . REPLY_SIZE));		
			if (strlen($response = mapi_read($socket)) > 0 ) {
				// something went wrong 
				$last_error = $response;
				return FALSE;
			}
		} else {
			return FALSE;
		}
		
		// Create a new connection instance and insert an entry in the connections table
		$id = mapi_generate_id();
		$connection = array("id" => $id, "socket" => $socket, "transactions" => array()) ;
		$connection_pool[] = $connection;
		
		return $connection;
	}
	
	function mapi_connected($conn=NULL) {
		global $last_error;
		global $connection_pool;
		
		if ($conn == NULL) {
			if (count($connection_pool == 1) && $connection_pool[0]["socket"] != NULL ) {
				return TRUE;
			}
			
		} else {
			// Create anonymous callback function to filter results for connection.
			$function_body = 'return ( $input[\'id\'] == "'.$conn["id"].'" );';
			$function_name = create_function('$input', $function_body);
			// Filter the results array using the anonymous callback function.
			$connection = array_filter($connection_pool, $function_name);
		
			
			if ($connection[0]["socket"] != NULL) {
				return TRUE;
			}
		}
		
		return FALSE;
	}
	
	/* Returns a pointer to the current (last initialized) connection */
	function &mapi_get_current_conn() {
		global $connection_pool;
		
		if (count($connection_pool) > 0) {
			return end($connection_pool);
		} else {
			return FALSE;
		}
	}
	
	/* Returns a socket */
	function mapi_open() {
		$socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
		if ($socket == FALSE) {
			$last_error = socket_strerror(socket_last_error());
			return FALSE;
		}
		
		return $socket;
	}
	
	function mapi_close($conn=NULL) {
		global $connection_pool;
		
		if ($conn == NULL) {
			if (count($connection_pool) == 1) {
				$socket = $connection_pool[0]["socket"];
				socket_close($socket);
				
				foreach ($connection_pool as $field) {
					if (isset($field)) {
						unset($field);
					}
				}
				unset($connection_pool);
				
				return TRUE;
			}
			
		} else {
			$socket = $conn["socket"];
			socket_close($socket);
			
			/* remove the $conn from the pool */	
			// Create anonymous callback function to filter results for connection.
			$function_body = 'return ( $input[\'id\'] != "'.$conn["id"].'" );';
			$function_name = create_function('$input', $function_body);
			// Filter the results array using the anonymous callback function.
			$connection_pool = array_filter($connection_pool, $function_name);
			
			if (isset($conn)) {
				foreach ($conn as $field) {
					if (isset($field)) {
						unset($field);
					}
				}
				unset($conn);
			}
			
			return TRUE;
		}
		
		return FALSE;
	}
	
	function mapi_generate_id(){
		global $connection_pool;
		
		$connections = array();
		foreach($connection_pool as $conn) {
			$connections[] = $conn["id"];
		}
		
		$id = hash("sha1", time());
		if (count($connections) > 0) {
			while (in_array($id, $connections) ) {
				$id = hash("sha1", time());
			}
		}
			
		return $id;
	}
	
	
	function format_query($query) {
		return "s" . $query . ";";
	}
	
	/* Write data through a socket; make sure that the buffer is actually flushed */
	function socket_flush($socket, $data) {
		$buf = 0;
		$bytes = strlen($data);
		
		if ($socket == NULL) {
			return FALSE;
		}
		
		while ( ($bytes - $buf) > 0 ) {
			$buf += socket_write($socket, substr($data, $buf, $bytes), $bytes - $buf);
			
			if ($buf == FALSE) {
				return FALSE;
			}
			
		}
		return $buf;
	}

?>