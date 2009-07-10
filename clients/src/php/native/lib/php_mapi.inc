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
	
	define("PROTOCOL", "9"); // supported protocol(s)
	
	$socket	= mapi_open();
	$handle = array("header" => "", "query" => "", "record_set" => array(), "operation" => -1);
	
	function mapi_query($data) {
		global $handle;
		global $socket;
		
		$operation = mapi_store($data);
		
		if ($operation == Q_TABLE || $operation == Q_BLOCK) {
			while ($handle["query"]["index"] < $handle["query"]["rows"]) {
				// export a new window
				write_command("export " . $handle["query"]["id"] . " " . $handle["query"]["index"] . " 10" );				
				$handle["query"]["index"] += 10;
	
				$operation = mapi_store(mapi_read());
				
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
		
		$rows = ""; // stores (partially) retrieved rows
				foreach ($data as $row){
			
			if ($row[0] == MSG_QUERY) {
				if ($row[1] == Q_TABLE) {
					$operation = Q_TABLE;
					// save info about the query
					$fields = split(" ", $row);
					$handle["query"] = array("id" => $fields[1], "rows" => $fields[2], "cols" => $fields[3], "index" => $fields[4]);
					
				} else if ($row[1] == Q_UPDATE) {
					$operation = Q_UPDATE;
				} else if ($row[1] == Q_CREATE) {
					$operation = Q_CREATE;
				} else if ($row[1] == Q_TRANSACTION) {
					$operation = Q_TRANSACTION;
				} else if ($row[1] == Q_BLOCK) {
					// add Q_BLOCK to the record set
					$operation = Q_BLOCK;
					
					foreach ($data as $row) {
						$rows = $rows . $row;
					}
				} else if ($row[1] == MSG_PROMPT) {
					// block of rows retrieved
					$record_set = array_merge($record_set, php_parse_tuples($rows));
					$rows = "";
				}
				
			} else if ($row[0] == MSG_SCHEMA_HEADER){
				// skip for now
				continue;
			} else if ($row[0] == MSG_TUPLE) {
				// process tuples
				$rows = $rows . $row;
			} else if ($row[0] == MSG_PROMPT) {
				// receive all the tuples
				$record_set = php_parse_tuples($rows);
				$rows = "";
			} 
			
		}
		
		$handle["record_set"] = array_merge($handle["record_set"], $record_set);
		
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

	/*
		$conn_opts is an array containing:
		username => 
		password =>
		host 	 =>
		port 	 =>
		database =>
		hash	 =>
	
	*/
	function mapi_connect() {
		global $socket;
		
		$host = "127.0.0.1";
		$port = "50000";
		
		$user = "monetdb";
		$passwd = "monetdb";
		$hash = "sha1";
		$dbname = "demo";
		
		if (socket_connect($socket, $host, $port) == FALSE) {
			print(socket_strerror(socket_last_error()));
			return FALSE;
		}
		
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
		

		$response = "";
		// $challenge[0] contains the salt
		mapi_authenticate($user, $passwd, $hash, $credentials[0], $dbname);
		$response = mapi_read();

		set_timezone();
		
		write_command("reply_size " . REPLY_SIZE);		
		
		return TRUE;
	}
	
	
	function mapi_authenticate($user, $passwd, $hash, $salt, $dbname) {
		$auth_string = "";
		
		$hashsum = hash(strtolower($hash), $passwd . $salt);
		
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
	function mapi_write($msg) {
		global $socket;
		
		$fb = strlen($msg);
		$sb = 0;
		
		$fb = $fb << 1; 
		$fb = $fb | 1;
		
		$msg = chr($fb) . chr($sb) . $msg;
		
		if (socket_write($socket, $msg) == FALSE) {
			print socket_strerror(socket_last_error());
			return -1;
		}
		
		return strlen($msg);
	}
	
	function set_timezone() {
		global $socket;
		$tz_offset = "'" . date('P') . "'"; /* Difference to Greenwich time (GMT) with colon between hours and minutes */
				
		$query = "sSET TIME ZONE INTERVAL " . $tz_offset . " HOUR TO MINUTE;";
	
		$buf = mapi_write($query);
					
		$response = mapi_read();
		
		if ($response == "") { 
			return TRUE; 
		} else { 
			
			print $response; 
			return FALSE; 
		}
	}
	
	function write_command($cmd) {
		global $socket;
		
		$buf = mapi_write("X" . $cmd . "\nX");

		$response = mapi_read();
		
		if ($response == "") { 
			return TRUE; 
		} else { 
			print $response; 
			return FALSE; 
		}
		
	}
	
	function mapi_connect_proxy() {
		
		return TRUE;
	}
	
	/* Returns a socket */
	function mapi_open() {
		$socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
		if ($socket == FALSE) {
			print socket_strerror(socket_last_error());
			return FALSE;
		}
		
		return $socket;
	}
	
	function mapi_close() {
		global $socket;
		
		socket_close($socket);
	}

?>