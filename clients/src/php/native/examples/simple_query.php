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
		$db = monetdb_connect($host = "127.0.0.1", $port = "50000", $database = "demo" , $username = "monetdb", $password = "monetdb" );

		$sql = stripslashes($_POST['query']);
		$res = monetdb_query($sql);
		while ( $row = monetdb_fetch_assoc($res) )
		{
			print "<pre>\n";
			print_r($row);
			print "</pre>\n";
		}
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
