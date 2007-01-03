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
  | Example demonstrating the capabilities of the monetdb module for     |
  | PHP. This file shows interaction through the basic commands          |
  | and the MIL query language.                                          |
  |                                                                      |
  | Author: Marcin Zukowski <marcin@cwi.nl>                              |
  |                                                                      |
  +----------------------------------------------------------------------+
*/

$GET_ROW=0;
$GET_ASSOC=1;
$GET_ARRAY=2;
$GET_OBJECT=3;
$get_names=array("monetdb_get_row", "monetdb_get_assoc", 
	"monetdb_get_array", "monetdb_get_object");

function tuple_get_next($handle, $get_type) {
	global $GET_ROW, $GET_ASSOC, $GET_ARRAY, $GET_OBJECT;
	switch($get_type) {
		case $GET_ROW:
			return monetdb_fetch_row($handle);
		case $GET_ASSOC:
			return monetdb_fetch_assoc($handle);
		case $GET_ARRAY:
			return monetdb_fetch_array($handle);
		case $GET_OBJECT:
			return monetdb_fetch_object($handle);
	}
	echo "UNKNOWN TYPE!\n";
}

function tuple_get_field($handle, $get_type, $tuple, $index)
{
	global $GET_ROW, $GET_ASSOC, $GET_ARRAY, $GET_OBJECT;
	switch($get_type) {
		case $GET_ROW:
			return $tuple[$index];
		case $GET_ASSOC:
			return $tuple[monetdb_field_name($handle, $index)];
		case $GET_ARRAY:
			/* either index or field name */
			if (index%2)
				return $tuple[$index];
			else 
				return $tuple[monetdb_field_name($handle, $index)];
		case $GET_OBJECT:
			return $tuple->{monetdb_field_name($handle, $index)};
	}
	echo "UNKNOWN TYPE!\n";
}

function print_query($query, $get_type)
{
	global $get_names ;
	
	echo "<hr width='100%'/>\n";
	echo "<hr width='100%'/>\n";
	echo "<p>Running <pre>$query</pre>\n";
	$q = monetdb_query($query);
	if ($q !== FALSE) {
		echo "SUCCESS</p>\n";
	} else { 
		die "FAIL, error message:<pre>". monetdb_last_error() ."</pre> Dying</p>\n";
	}
	
	$nr=monetdb_num_rows($q);
	$nf=monetdb_num_fields($q);
	echo "<p>Query result: $nf fields, $nr rows</p>\n";
	echo "<p>Field details:</p>\n";
	echo "<table border='1'>\n";
	echo "  <tr><th>Name</th><th>Type</th></tr>\n";
	for($i=0;$i<$nf;$i++) {
		$name = monetdb_field_name($q, $i);
		$type = monetdb_field_type($q, $i);
		echo "  <tr><td>$name</td><td>$type</td></tr>\n";
	}
	echo "</table>\n";
	
	echo "<p>Result content(using <b>". ($get_names[$get_type]) . "()</b> calls)</p>\n";
	
	echo "<table width='100%' border='1'>\n";
	echo "  <tr>\n";
	for($i=0;$i<$nf;$i++) {
		echo "    <th>".(monetdb_field_name($q, $i))."</th>\n";
	}
	echo "  </tr>\n";
	while($tuple=tuple_get_next($q, $get_type)) {
		echo "  <tr>\n";
		for($i=0;$i<$nf;$i++) {
			echo "    <td>".(tuple_get_field($q, $get_type, $tuple, $i))."</td>\n";
		}
		echo "  </tr>\n";
	}
	echo "</table>\n";
}

/* ========================================================================= */

$MONETDB_NAME="monetdb";

if (!defined("PHP_SHLIB_SUFFIX")) define("PHP_SHLIB_SUFFIX", "so");

echo "<p>Testing for '$MONETDB_NAME'... ";
if(!extension_loaded($MONETDB_NAME)) {
	echo "NOT LOADED, trying to load</p>\n";
	dl($MONETDB_NAME . '.' . PHP_SHLIB_SUFFIX);
} else {
	echo "LOADED</p>\n";
}

echo "<p>Testing for '$MONETDB_NAME' again... ";
if(!extension_loaded($MONETDB_NAME)) {
	die "NOT LOADED, dying</p>\n";
} else {
	echo "LOADED</p>\n";
}

$functions = get_extension_funcs($MONETDB_NAME);
echo "<p>Functions available in '$MONETDB_NAME' module:</p>\n";
echo "<ul>\n";
foreach($functions as $func) {
    echo "<li>$func</li>\n";
}
echo "</ul>\n";

echo "<p>'$MONETDB_NAME' variable values</p>\n";
$monet_vars = array("default_port","default_language","default_hostname",
	"default_username", "default_password");
echo "<table border='1'>\n";
foreach($monet_vars as $var) {
	echo "<tr><td>monetdb.$var</td><td>". ini_get("monetdb.".$var) ."</td></tr>\n";
}
echo "</table>\n";

echo "<p>Connecting to MonetDB server with default values... ";
$db = monetdb_connect();
if ($db !== FALSE) {
	echo "SUCCESS</p>\n";
} else {
	die "FAIL, dying</p>\n";
}

print_query("ls();", $GET_ROW);

print_query("dir();", $GET_ARRAY);

print_query("new(int,flt).insert(1,1.1).insert(2,2.2).insert(3,3.3).insert(4,4.4).print();", $GET_ASSOC);

print_query("bat(\"adm_atomtbl\").reverse().print();", $GET_OBJECT);

echo "<hr><center>T H E &nbsp; &nbsp; E N D</center><hr/>\n";

phpinfo();
?>
