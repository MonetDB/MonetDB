<?php
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
?>

<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">

<html xmlns="http://www.w3.org/1999/xhtml"" xml:lang="en" lang="en">

<head>
	<title>MonetDB Query</title>
</head>

<body>

<?php
	require '../lib/php_monetdb.php';
	if ( isset($_POST['query']) )
	{
		$db = monetdb_connect($lang = "sql", $host = "127.0.0.1", $port = "50000" , $username = "monetdb", $password = "monetdb", $database = "php_demo" )
		or die(monetdb_last_error());
		
		$sql = monetdb_escape_string($_POST['query']);
		$res = monetdb_query($sql);
		while ( $row = monetdb_fetch_assoc($res) )
		{
			print "<pre>\n";
			print_r($row);
			print "</pre>\n";
		}
		
		monetdb_disconnect();
	}

	print "<form method=\"post\" action=\"{$_SERVER['PHP_SELF']}\">\n";
	print "<label for=\"query\">SQL Query:</label>\n";
	print "<input type=\"text\" name=\"query\" id=\"query\"
	value=\"{$_POST['query']}\" />\n";
	print "<input type=\"submit\" value=\"Execute\" />\n";
	print "</form>\n";
?>	

</body>

</html>
