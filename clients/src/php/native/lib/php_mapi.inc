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
   * -  write_command($cmd) {
   * -  mapi_connect_proxy() {
   * -  mapi_open() {
   * - mapi_close() {
   *
**/

	date_default_timezone_set('Europe/Amsterdam'); /* Set the default timezone. TODO: read from php.ini */
	
	define("MAX_PACKET_SIZE", 32767); // Maximum packet size 
	
	define("REPLY_SIZE", "-1"); // Set the initial reply size
	
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
	
	$socket	= mapi_open();
	$connected = FALSE; // Connection to a db status;
	$last_error = "";
	$handle = array("header" => array(), "query" => "", "record_set" => array(), "operation" => -1);
	
	function mapi_query($data) {
		global $handle;
		global $socket;
		
		$operation = mapi_store($data);
		
		print $data;
		
		/* The query produced a record set */
		if ($operation == Q_TABLE || $operation == Q_BLOCK) {
			while ($handle["query"]["index"] < $handle["query"]["rows"]) {
				// export a new window
				$left_rows = $handle["query"]["rows"] - $handle["query"]["index"];
				
				$exp_size = min(10, $left_rows);
				write_command("export " . $handle["query"]["id"] . " " . $handle["query"]["index"] . " " . $exp_size );				
				$data = mapi_read();
				
				$operation = mapi_store($data);
				
				$handle["query"]["index"] += $exp_size;
				
			}
		} 
		// record the last operation succesfully executed.
		$handle["operation"] = $operation;
		return $handle;
	}
	
	function mapi_store($data) {
		global $handle;
				
		$data = explode("\n", $data);

		$record_set = "";
		$operation = "";
				
		$header = ""; // store temporary header informations;
		
		$rows = ""; // stores (partially) retrieved rows
		foreach ($data as $row) {			
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
				// skip for now
				
				$header = $header . $row . "\n";
			} else if ($row[0] == MSG_TUPLE) {
				// process tuples
				$rows = $rows . $row;
			} else if ($row[0] == MSG_PROMPT) {
				// receive all the tuples
				$record_set = php_parse_tuples($rows);
				$rows = "";
			} 
			
		}
		
		//print_r($record_set);
		if ($record_set != "") {
			$handle["record_set"] = array_merge($handle["record_set"], $record_set);
		}
		
		if ($header != "") {
			php_parse_header($header);
		}
		
		
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
		global $handle;
		$header = explode("\n", $header);
		$name = $header[0];
		
		/* Field names */
		$header[1] = ltrim($header[1], "%");
		$header[1] = split("#", $header[1]);
		$handle["header"]["fields"] = explode(",\t", $header[1][0]);


		/* Field types */
		$header[2] = ltrim($header[2], "%");
		$header[2] = split("#", $header[2]);
		$handle["header"]["types"]	= explode(",\t", $header[2][0]);
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
	function mapi_connect($options, $merovingian_iter) {
		global $socket;
		global $last_error;
		
		$host 	= $options["host"];
		$port 	= $options["port"];

		$user 	= $options["username"];
		$passwd = $options["password"];
		$hash 	= $options["hashfunc"];
		$dbname = $options["database"];
		
		# get server challenge
		$challenge = mapi_read();				

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
			mapi_authenticate_v8($user, $passwd, $hash, $credentials[0], $dbname);
		} else if ($credentials[2] == PROTOCOL_v9) {
			// $credentials[5] contains pwhash
			mapi_authenticate_v9($user, $passwd, $hash, $credentials[0], $dbname, $credentials[5]);			
		} else {
			$last_error = "Protocol " . $credentials[2] . " not supported. Aborting.";
			return FALSE;
			
		}
		
		$response = mapi_read();
		
		if ($response != MSG_PROMPT) { 
		// not ready to authenticate yet
			if ($response[0] == MSG_REDIRECT) {
				
				if ($credentials[1] == "merovingian") {
					if ($merovingian_iter < MAX_MEROVINGIAN_ITER) {
						$merovingian_iter++;
						mapi_connect($options, $merovingian_iter);
					}
				
				}
				else if ($credentials[1] == "mserver") {
					mapi_close();
					$socket = mapi_connect();
					mapi_connect($options);
				}
				
			} else if ($response[0] == MSG_INFO) {
				$last_error = $response;
			}
		
		
		}
		
		return TRUE;
	}
	
	
	
	function mapi_authenticate_v8($user, $passwd, $hash, $salt, $dbname) {
		$auth_string = "";
		
		if (strtolower($hash) != "plain") {
			$hashsum = hash(strtolower($hash), $passwd . $salt);
		} else {
			$hashsum = $passwd+$salt;
		}
		
		
		$auth_string = "BIG:" . $user . ":{" . strtoupper($hash) . "}" . $hashsum . ":sql:" . $dbname . ":";
		
		mapi_write($auth_string);
	}

	function mapi_authenticate_v9($user, $passwd, $hash, $salt, $dbname, $pwhash) {
		$auth_string = "";
	
		$pwhashsum = hash(strtolower($pwhash), $passwd);
		$hashsum   = hash(strtolower($hash), $pwhashsum . $salt);
		
		$auth_string = "BIG:" . $user . ":{" . strtoupper($hash) . "}" . $hashsum . ":sql:" . $dbname . ":";
		
		mapi_write($auth_string);
	}

	# decode the header and get the requested amount of data
	function mapi_read() {
		global $socket;

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


	# encode data and send it to the server. Returns the number of bytes sent.
	function mapi_write1($msg) {
		global $socket;
		global $last_error;
		
		$fb = strlen($msg);
		$sb = 0;
		
		$fb = $fb << 1; 
		$fb = $fb | 1;
		
		$msg = chr($fb) . chr($sb) . $msg;
		
		if (socket_write($socket, $msg) == FALSE) {
			$last_error = socket_strerror(socket_last_error());
			return -1;
		}
		
		return strlen($msg);
	}


	function mapi_write($msg) {
		global $socket;
		global $last_error;
		
		$fb = 0;
		$sb = 0;
		
		$buf = 0;
		
		$is_final = FALSE;
		while (! $is_final) {
		
			if (strlen($msg) < MAX_PACKET_SIZE) {
				$is_final = TRUE;
				$data = $msg;
				$fb = (strlen($data) << 1) | 1;
				
			} else {
				$data  = substr($msg, $fb, min(MAX_PACKET_SIZE, len($msg) - $fb)  );
				
				$fb = (strlen($data) << 1) | 0; // more packets will follow
				
			}
			
		
			
			if ($buf += socket_write($socket, chr($fb) . chr(0) . $data) == FALSE) {
				$last_error = socket_strerror(socket_last_error());
				return -1;
			}
		
		}
		
		return $buf;
	}


	
	function set_timezone() {
		global $socket;
		global $last_error;

		$tz_offset = "'" . date('P') . "'"; /* Difference to Greenwich time (GMT) with colon between hours and minutes */
				
		$query = "sSET TIME ZONE INTERVAL " . $tz_offset . " HOUR TO MINUTE;";
	
		$buf = mapi_write($query);
					
		$response = mapi_read();
		
		if ($response == "") { 
			return TRUE; 
		} else { 
			
			$last_error = $response; 
			return FALSE; 
		}
	}
	
	function write_command($cmd) {
		global $socket;
		
		$buf = mapi_write("X" . $cmd . "\nX");
		
/*
		$response = mapi_read();
		
		if ($response == "") { 
			return TRUE; 
		} else { 
			print "RESPONSE" . $response; 
			return FALSE; 
		}
*/		
	}
	
	
	function mapi_connect_proxy($options) {
		global $socket;
		global $connected;
		global $last_error;
		
		$merovingian_iter = 0;
		
		$host 	= $options["host"];
		$port 	= $options["port"];

		$user 	= $options["username"];
		$passwd = $options["password"];
		$hash 	= $options["hashfunc"];
		$dbname = $options["database"];
		

		if (socket_connect($socket, $host, $port) == FALSE) {
			$last_error = socket_strerror(socket_last_error());
			return FALSE;
		}
		
		if ( ($connected = mapi_connect($options, $merovingian_iter)) == TRUE ) {
			/* Connected */
			
			set_timezone(); // arbitrarly set the timezone
			
			// export the reply size (max number of tuples returned at query executions)
			write_command("reply_size " . REPLY_SIZE);		
			if (strlen($response = mapi_read()) > 0 ) {
				// something went wrong 
				$last_error = $response;
				return FALSE;
			}
		} else {
			return FALSE;
		}
		
		return TRUE;
	}
	
	function mapi_connected() {
		global $connected;
		global $last_error;
		
		return $connected;
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
	
	function mapi_close() {
		global $socket;
		global $connected;
		
		socket_close($socket);
		$connected = FALSE;
	}

?>