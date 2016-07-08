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

	define("MAX_PACKET_SIZE", 8190); // Maximum packet size

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

	define("PROTOCOL_v9", 9); // supported protocol(s)

	define("LANG_SQL", "sql");

	define("MEROVINGIAN", "merovingian");
	define("MSERVER", "mserver");
	define("MONETDB", "monetdb");

	define('MAX_MEROVINGIAN_ITER', 10); // Maximum number of iterations during proxied auth through merovingian

	define('PLACEHOLDER', '?'); // Used for string escaping.

	/**
	* $connection_pool contains a list of the active database connections
	*/
	$connection_pool = array();

	/**
	* Stores the last error reported by the server
	*/
	$last_error = "";


	/**
	* Execute an SQL query and return the resulting handle by reference.
	*/
	function mapi_execute($conn=NULL, $query) {
		global $connection_pool;
		global $rows;
		/**
		* Query handle
		*
		* "conn" - id of the connection that fired the query
		* "header" - table header
		* "record_set" - retrieved record set (if present)
		* "operation" - query type
		*
		*/

		// if multiple connections are present, require the user to specify which one to use.
		if ($conn == NULL) {
			return FALSE;
		} else if (($socket = $conn["socket"]) == NULL) {
			return FALSE;
		}

		/* Fire the query and read back the response */
		$buf = mapi_write($socket, format_query($query, $conn["lang"]));
		$data = mapi_read($socket);

		if ($conn['lang'] == LANG_SQL) {
			$handle = array("conn" => "", "header" => array(), "query" => "", "record_set" => array(), "operation" => -1, "last_row" => 0);
			$handle["conn"] = $conn["id"];

			if ( ($operation = mapi_store($data, $handle)) == FALSE) {
				return FALSE;
			}

			/* The query produced a record set */
			if ($operation == Q_TABLE || $operation == Q_BLOCK) {
				// fetch the whole result set
				if ($handle["query"]["index"] < $handle["query"]["rows"]) {
					mapi_fetch_next($conn, $handle);
				}
			}

			if ($handle == NULL) {
				return FALSE;
			}

			return $handle;
		}
	}

	function mapi_fetch_next($conn, &$handle){
		if ($conn['socket'] == NULL){
			return FALSE;
		}
		$socket = $conn['socket'];

		$offset = 10;
		while ($handle["query"]["index"] < $handle["query"]["rows"]) {
			// export a new window
			$left_rows = $handle["query"]["rows"] - $handle["query"]["index"];

			$exp_size = min($offset, $left_rows);
			$offset += 100;
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
		// global $rows;

		$data = explode("\n", $data);

		$operation = "";
		$header = ""; // store temporary header informations;

		$rows = ""; // stores (partially) retrieved rows
		foreach ($data as $row) {
			/*
			 	PHP5.2 complains when $row[0] is accessed with:

				Notice: Uninitialized string offset: 0

				In order to avoid the E_NOTICE error substr($row, 0, 1) is used
				to access the first character of a string

			*/
			if (substr($row, 0, 1) == MSG_QUERY) {
				if ($row[1] == Q_TABLE) {
					$operation = Q_TABLE;
					// save info about the query
					$fields = explode(" ", $row);
					$handle["query"] = array("id" => $fields[1], "rows" => $fields[2], "cols" => $fields[3], "index" => $fields[4]);

				} else if ($row[1] == Q_UPDATE) {
					$operation = Q_UPDATE;
					$fields = explode(" ", $row);
					$handle["query"] = array("affected" => $fields[1]);
				} else if ($row[1] == Q_CREATE) {
					$operation = Q_CREATE;
				} else if ($row[1] == Q_TRANSACTION) {
					$operation = Q_TRANSACTION;
				} else if ($row[1] == Q_BLOCK) {
					// add Q_BLOCK to the record set
					$operation = Q_BLOCK;
				}
			} else if (substr($row, 0, 1) == MSG_SCHEMA_HEADER){
				// process the table header
				$header = $header . $row . "\n";
			} else if (substr($row, 0, 1) == MSG_TUPLE) {
				// process tuples
				$rows .= $row;
			} else if (substr($row, 0, 1) == MSG_PROMPT) {
				fast_array_merge($handle["record_set"], php_parse_tuples($rows));
			} else if (substr($row, 0, 1) == MSG_INFO) {
				$last_error = $row;
				return FALSE;
			}
		}
		/*
		if ($record_set != "") {
			//$handle["record_set"] = array_merge($handle["record_set"], $record_set);
			$handle["record_set"] = $handle["record_set"] . $record_set;
		}*/

		if ($header != "") {

			$handle["header"] = php_parse_header($header);
			/* Store the number of fields returned by the dataset */
		   	if ($operation == Q_TABLE || $operation == Q_BLOCK ) {
				$handle["query"]["fields"] = count($handle["header"]["fields"]);
			}
		}

		$handle["operation"] = $operation;

		return $operation;
	}

	/* Concatenates two arrays in place */
	function fast_array_merge(&$a1, $a2) {
		foreach ($a2 as $row) {
			$a1[] = $row;
		}
	}

	function php_parse_tuples($rows) {
		//$parsed_rows = "";

		$rows = rtrim($rows, "\t]");
		return explode("\t]", $rows);
//		print_r($parsed_rows);
		/*
		foreach ($rows as &$row) {

			$row = ltrim($row, "[ ");
			$row = explode(",\t",  $row);

			foreach ($row as &$field) {
				$field = stripslashes($field);
				// strip left/right \" chars and right ','
				$field = rtrim($field, '"');
				$field = ltrim($field, '"');
			}
			$parsed_rows[] = $row;
		}
		*/
	//	return $parsed_rows;
	}

	function php_parse_row($row) {
		$row = ltrim($row, "[ ");
		$row = explode(",\t",  $row);
		foreach ($row as &$field) {
			if ($field == "NULL") {
				$field = NULL;
			}
			else {

				$field = stripslashes($field);
				// strip left/right \" chars and right ','
				$field = rtrim($field, '"');
				$field = ltrim($field, '"');
			}
		}
		return $row;
	}


	function php_parse_header($header) {
		$header = explode("\n", $header);
		$name = $header[0];

		$header_array = array();

		/* Field names */
		$header[1] = ltrim($header[1], "% ");
		$header[1] = explode("#", $header[1]);
		$header_array["fields"] = explode(",\t", $header[1][0]);

		/* Field types */
		$header[2] = ltrim($header[2], "% ");
		$header[2] = explode("#", $header[2]);
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
	function mapi_connect(&$socket, &$options, $merovingian_iter=NULL) {
		global $last_error;
		global $connection_pool;

		$host 	= $options["host"];
		$port 	= $options["port"];

		$user 	= $options["username"];
		$passwd = $options["password"];
		$hash 	= $options["hashfunc"];
		$dbname = $options["database"];

		$lang 	= $options["lang"];


		/* No merovingian redirect. Perform an actual connection. */
		if ($merovingian_iter == NULL) {
			if (socket_connect($socket, $host, $port) == FALSE) {
            	$last_error = socket_strerror(socket_last_error());
              	throw new Exception('Cannot connect to server: ' . $last_error);
            }
		}


		// get server challenge
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
		$credentials = explode(":", $challenge);
		$algos = explode(',', $credentials[3]);


		// $challenge[0] contains the salt
		if ($credentials[2] == PROTOCOL_v9) {
			// $credentials[5] contains pwhash
			// $credentials[0] contains the hash
			mapi_authenticate_v9($socket, $user, $passwd, $hash, $algos, $credentials[0], $dbname, $credentials[5]);
		} else {
			$last_error = "Protocol " . $credentials[2] . " not supported. Aborting.";
			return FALSE;
		}

		$response = mapi_read($socket);

		/* Follow the first redirect */

		if ($response != MSG_PROMPT) {
		// not ready to authenticate yet
			if ($response[0] == MSG_REDIRECT) {
				$redirects = explode("\n", $response);
	                 	/* Follow the first redirect */

				if ( ($redirects[0] == "") || (substr($redirects[0], 0, 6) != "^mapi:") ) {
					print "Invalid redirect " . $redirects[0] . "\n";
					return FALSE;
				}

				$link = substr($redirects[0], 6, strlen($redirects[0]));
				$redirect_to = parse_url($link);
				//print_r($redirect_to);
				if ($redirect_to['scheme'] == MEROVINGIAN) {
					if ($merovingian_iter < MAX_MEROVINGIAN_ITER) {
						$merovingian_iter++;
						if (mapi_connect($socket, $options, $merovingian_iter) == FALSE) {
							return FALSE;
						}
					} else {
						$last_error = "Maximum number of merovingian iterations exceeded during authentication\n";
						return FALSE;
					}
				}
			 	else if($redirect_to['scheme'] == MONETDB ) {

					$options['host'] = $redirect_to['host'];
					$options['port']     = $redirect_to['port'];
					$options['database'] = ltrim($redirect_to['path'], '/');

					socket_close($socket);
					$socket = mapi_open();
					if (mapi_connect($socket, $options) == FALSE) {
						return FALSE;
					}

			    } else {
					print $response;
					return FALSE;
				}

			} else if ($response[0] == MSG_INFO) {
				$last_error = $response;
				return FALSE;
			}
		}

		return TRUE;
	}

	/* Hash function names have to be uppercase */
	function mapi_authenticate_v9($socket, $user, $passwd, $hash, $algos, $salt, $dbname, $pwhash) {
		$auth_string = "";
		if ( (is_array($algos) && (! in_array(strtoupper($hash), $algos)) )  ) 			 {
			$last_error = "Hash function " . $hash . " not supported";
			return FALSE;
		}

		$pwhashsum = hash(strtolower($pwhash), $passwd);
		$hashsum   = hash(strtolower($hash), $pwhashsum . $salt);

		$auth_string = "BIG:" . $user . ":{" . strtoupper($hash) . "}" . $hashsum . ":sql:" . $dbname . ":";

		mapi_write($socket, $auth_string);
	}

	// decode the header and get the requested amount of data
	function mapi_read($socket=NULL) {
		# get the first 2 bytes
		if ( ($header = socket_read($socket, 2)) == FALSE) {
			$last_error = socket_strerror(socket_last_error());
			throw new Exception('Lost connection with the server: ' . $last_error);
		}
		$data = "";

		$chunk_size = ((ord($header[1]) << 7) | (ord($header[0]) >> 1));
		// keep reading until we have everything
		while (strlen($data) < $chunk_size) {
			$data .= socket_read($socket, $chunk_size - strlen($data));
		}


		while ((ord($header[0]) & 1) == 0 ) {
			if ( ($header = socket_read($socket, 2)) == FALSE) {
				$last_error = socket_strerror(socket_last_error());
				throw new Exception('Lost connection with the server: ' . $last_error);
			}

			$chunk_size = ( ((ord($header[1]))  << 7) | (ord($header[0]) >> 1) );

			$block = "";
			while (strlen($block) < $chunk_size) {
				if ( ($block .= socket_read($socket, $chunk_size - strlen($block))) == FALSE) {
					$last_error = socket_strerror(socket_last_error());
					throw new Exception('Lost connection with the server: ' . $last_error);
				}
			}


			$data = $data . $block;
		}
		if (strlen($data) == 0) {
			return "";
		}

		return $data;
	}


	// encode data and send it to the server. Returns the number of bytes sent.
	function mapi_write($socket=NULL, $msg) {
		// print "Msg_len: " . strlen($msg) . "\n";
		global $last_error;

		$fb = 0;
		$sb = 0;

		$pos = 0;
		$data = "";

		$buf = 0;

		$is_final = FALSE;
		while (! $is_final) {
			$data  = substr($msg, $pos, min(MAX_PACKET_SIZE, (strlen($msg) - $pos))  );
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

		$buf = mapi_write($socket, format_query($query, LANG_SQL)); // set_timezone is called only when connecting to an sql db

		$response = mapi_read($socket);

		if ($response == "") {
			return TRUE;
		} else {

			$last_error = $response;
			return $response;
		}
	}

	function format_command($cmd) {
		return "X" . $cmd;
	}


	function mapi_connect_proxy(&$options) {
		global $last_error;
		global $connection_pool;
		global $pconnect_count;

		$merovingian_iter = 0;

		/**
		  * When connecting, the function would first try to find a (persistent) link that's already
		  * open with the same host, username and password. If one is found, an identifier for it will be returned
		  * instead of opening a new connection.
		  * TODO: move this check to mapi_connect() to deal with options arrays rewritten by redirects.
		*/
		if ($options['persistent'] == TRUE) {
			if (count($connection_pool) > 0) {
				foreach ($connection_pool as $conn) {
					if ( ($conn["persistent"] == TRUE) && ($conn["dbname"] == $options['database']) && ($conn['username'] == $options['username']) && ($conn["password"] == hash($options['hashfunc'], $options['password']) && ($conn['host']) == $options['host']) && ( $conn['port'] == $options['port'] ) ) {
						return $conn;
					}
				}
			}
		}

		$socket	= mapi_open();

		if ( mapi_connect($socket, $options, $merovingian_iter) == TRUE ) {
			/* Connected */

			// Create a new connection instance and insert an entry in the connections table
			$id = mapi_generate_id();
			if ($options['lang'] == LANG_SQL) {


				/*
					PHP requires a timezone to be specified in the configuration environment; if not specified
					either in php.ini or via date_default_timezone_set(), a default is set 'Europe/Berlin'.

					PHP complains in case we query the OS to get system's timezone (E_STRICT error).

					To avoid unexpected behaviours and warnings at execution time we set a timezone on mserver
					only if PHP interpreter is aware of it (php.ini contains a date.timezone entry).
				*/
				if (ini_get("date.timezone")) {
					set_timezone($socket); // set the timezone according to the system's configuration
				}

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
		} else {
			socket_close($socket);
			return FALSE;
		}

		$connection = array("id" => $id, "socket" => $socket, "host" => $options["host"], "port" => $options["port"], "dbname" => $options['database'], "username" => $options["username"], "password" => hash($options['hashfunc'], $options["password"]), "transactions" => array(), "persistent" => $options["persistent"], "lang" => $options['lang']);

		$connection_pool[] = $connection;

		return $connection;
	}

	function mapi_connected($conn=NULL) {
		global $last_error;
		global $connection_pool;

		if ($conn == NULL) {
			return FALSE;
		} else {
			return mapi_ping($conn);
		}

		return FALSE;
	}

	function mapi_ping($conn=NULL) {
		if ($conn != NULL) {
			switch($conn['lang']) {
				case LANG_SQL:
					$res = mapi_execute($conn, "select true;");
					break;
			}
			if ($res != NULL) {
				if (!is_array($res['query']) ||
					!array_key_exists('id', $res['query']) ||
					mapi_free_result($conn['id'], $res['query']['id']))
			   	{
					return TRUE;
				}
			}
		}
		return FALSE;
	}

	/* Returns a pointer to the current (last initialized) connection */
	function mapi_get_current_conn() {
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

		/* TODO: start closing connection at the end of the array! */
		if ($conn == NULL) {
			if ((count($connection_pool) == 1) && ($connection_pool[0]["persistent"]) == FALSE) {
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

	function mapi_free_result($conn_id, $res_id) {
		global $connection_pool;
		global $last_error;
		/* Fetch the connection from the pool */

		$conn = NULL;
		foreach ($connection_pool as $connection) {
			if ($connection["id"] == $conn_id) {
				$conn = $connection;
				break;
			}
		}

		if ($conn == NULL) {
			return FALSE;
		}

		$socket = $conn["socket"];

		/* Send a close command */
		$cmd = "close " . $res_id;

		mapi_write($socket, format_command($cmd));
		$last_error = mapi_read($socket);

		if ($last_error != "") {
			return FALSE;
		}

		return TRUE;
	}


	function mapi_generate_id(){
		global $connection_pool;

		$connections = array();
		if ($connection_pool !== null) {
		    foreach($connection_pool as $conn) {
			$connections[] = $conn["id"];
		    }
		}

		$id = hash("sha1", time());
		if (count($connections) > 0) {
			while (in_array($id, $connections) ) {
				$id = hash("sha1", time());
			}
		}

		return $id;
	}


	function format_query($query, $lang) {
		if ($lang == LANG_SQL) {

			return "s" . $query . "\n;";
		}
		return FALSE;
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
			//print "Buf: " . $buf . "\n";

			if ($buf == FALSE) {
				return FALSE;
			}

		}
		return $buf;
	}

	function mapi_quote($string, $size) {
		$quoted_string = ""; # upper bound: $size * 2 + 1

		$index = 0; // current position in the original string
		$t = 0; // current position in the quoted string

		/* Parse the original string character by character and copy it in a new buffer escaping characters */
        while ($size < 0 ? $quoted_string[$t] : $size > 0) {
                if ($size > 0)
                        $size--;
                switch ($string[$index]) {
                	case '\n':
                        $quoted_string[$t++] = '\\';
                        $quoted_string[$t++] = 'n';
                        break;
                	case '\t':
                        $quoted_string[$t++] = '\\';
                        $quoted_string[$t++] = 't';
                        break;
                	case PLACEHOLDER:
                        $quoted_string[$t++] = '\\';
                        $quoted_string[$t++] = PLACEHOLDER;
                        break;
                	case '\\':
                        $quoted_string[$t++] = '\\';
                        $quoted_string[$t++] = '\\';
                        break;
                	case '\'':
                        $quoted_string[$t++] = '\'';
                       break;
                	case '\"':
                        $quoted_string[$t++] = '\\';
                        $quoted_string[$t++] = '"';
                        break;
                	case '\0':
                        $quoted_string[$t++] = '\\';
                        $quoted_string[$t++] = '0';
                        break;
                default:
                        $quoted_string[$t++] = $string[$index];
                        break;
                }
                $index++;
                /* also deal with binaries */
        }

		if (is_array($quoted_string)) {
			$quoted_string = implode($quoted_string);
		}
        return $quoted_string;
	}

?>
