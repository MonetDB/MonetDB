<?php
# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
#
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
#
# The Original Code is the Monet Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2004 CWI.
# All Rights Reserved.
?>

<?php

class Mapi {
	function Mapi($server, $port, $user) {
		$this->server = $server;
		$this->port = $port;
		$this->user = $user;
		$this->socket = socket_create(AF_INET, SOCK_STREAM, 0);
		if (!socket_connect($this->socket, $server, $port))
			echo "server refuses access\n";

		$this->buf = "";
		$this->prompt = "";
		$this->cmd_intern("$user\n");
		$this->result();
	}

	function disconnect() {
		$this->cmd_intern("quit;\n");
		socket_close($this->socket);
	}

	function cmd_intern($cmd) {
		socket_write($this->socket, $cmd, strlen($cmd));
	}

	function getstring() {
		$str = "";
		while (!($index = strpos($this->buf, "\001"))) {
			$str .= $this->buf;
			$len = 4096;
			$this->buf = socket_recv($this->socket, $len, 4096, 0);
		}
		$str .= substr($this->buf, 0, $index);
		$this->buf = substr($this->buf, $index + 1, strlen($this->buf));
		return $str;
	}

	function result() {
		$res = $this->getstring();
		$this->getprompt();
		return $res;
	}

	function getprompt() {
		$prompt = $this->getstring();
	}

	function cmd($cmd) {
		$cmd = "$cmd\n";
		$this->cmd_intern($cmd);
		return $this->result();
	}
}
?>
