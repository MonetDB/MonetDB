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
require("Mapi.php");

class MapiClient extends Mapi {
	function MapiClient() {
		$this->Mapi($this->hostname(), $this->portnr(), "niels");
	}

	function mapiport_intern() {
		$mapiport = 'localhost:50000';
		if (getenv("MAPIPORT"))
			$mapiport = getenv("MAPIPORT"); 
		return $mapiport;
	}

	function hostname() {
		ereg("([a-zA-Z_]*)(:[0-9]*|$)",
		     $this->mapiport_intern(), $res);
		$hostname = $res[1];
		if ($hostname == '')
			$hostname = 'localhost';
		return $hostname;
	}

	function portnr() {
		ereg("([a-zA-Z_]*)(:[0-9]*|$)",
		     $this->mapiport_intern(), $res);
		$portnr = $res[2];
		if ($portnr == '')
			$portnr = 50000;
		else
			$portnr = substr($portnr,1, strlen($portnr) - 1);
		return $portnr;
	}
}

$monet = new MapiClient;
echo $monet->cmd($HTTP_POST_VARS["cmd"]);
$monet->disconnect();
?>

<FORM action="MapiClient.php" method="post">
<input type=text name=cmd>
</FORM>
