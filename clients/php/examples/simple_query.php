<?php
# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2014 MonetDB B.V.
# All Rights Reserved.
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
