<?php
# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.
?>

<?php
/*
  +----------------------------------------------------------------------+
  | PHP Version 4                                                        |
  +----------------------------------------------------------------------+
  | Copyright (c) 1997-2003 The PHP Group                                |
  +----------------------------------------------------------------------+
  | This source file is subject to version 2.02 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available at through the world-wide-web at                           |
  | http://www.php.net/license/2_02.txt.                                 |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  |                                                                      |
  | Example demonstrating the capabilities of the MonetDB wrapper for    |
  | The PEAR database abstraction layer (pear.php.net)                   |
  |                                                                      |
  | Author: Arjan Scherpenisse <A.C.Scherpenisse@CWI.nl                  |
  |                                                                      |
  +----------------------------------------------------------------------+
*/

# if monetdb.php isn't installed into the correct pear/DB directory
# we load first 
#require("DB/monetdb.php");
require("DB.php");

function printtable($resultset) {
    echo "<table border='1'>";
    echo "<tr>";
    foreach(array_keys($resultset[0]) as $head) 
	echo "<th>".ucfirst($head)."</th>";
    echo "</tr>";
    
    foreach($resultset as $row) {
	echo "<tr>";
	foreach($row as $k=>$v)
	    echo "<td>${v}</td>";
	echo "</tr>";
    }
    echo "</table>";
}

function check($result, $message = "MonetDB query failed") {
    if (DB::isError($result)) {
	echo $message . ". ";
	if ($result->message) echo "Message was: " . $result->message;
	die;
    }
}


// Connect to monet giving a user/password/host; default language is SQL. 
$db = DB::connect("monetdb://monetdb:monetdb@localhost/");
check($db, "Connection refused");

echo "<h1>Welcome to MonetDB/SQL in PHP/PEAR</h1>";

$db->setFetchMode(DB_FETCHMODE_ASSOC);
$db->autoCommit(false);

echo "<p>Creating test table: ";
check($db->query("CREATE TABLE testtable (name VARCHAR(255), zipcode VARCHAR(10), registered BOOLEAN);"));
check($db->query("INSERT INTO testtable VALUES('Arjan Scherpenisse', '1092 GJ', true);"));
check($db->query("INSERT INTO testtable VALUES('Test', '123', false);"));
echo "<strong>OK</strong>";

// Get table names
check($tables = $db->getCol($db->getSpecialQuery("tables"))); // this returns only USER defined tables. No views/system tables.

echo "<p>The following tables are defined: <strong>" . implode(", ", $tables) . "</strong>";

echo "<p>Contents of testtable:";
check($all = $db->getAll("SELECT * FROM testtable;"));
printtable($all);
 
$db->disconnect();

// Connect using the MIL language; take default values for user/pw/host
$db = DB::connect("monetdb(mil)://");
$db->setFetchMode(DB_FETCHMODE_ASSOC);
check($db, "Connection refused");

echo "<h1>Welcome to MonetDB/MIL in PHP/PEAR</h1>";

echo "<p>Making a test table:";
check($all = $db->getAll('(new (int, str)).insert(1, "Row 1").insert(2, "Next row").insert(3, "Another row").print();'));
printtable($all);

print "<p>Output of <tt>ls;</tt>:";
check($all = $db->getAll("ls();"));
printtable($all);


echo "<p>Bye bye.";

?>
