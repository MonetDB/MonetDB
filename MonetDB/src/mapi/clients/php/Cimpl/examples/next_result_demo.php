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
  | Example demonstrating the monetdb_next_result() function.            |
  |                                                                      |
  | Author: Arjan Scherpenisse <A.C.Scherpenisse@CWI.nl>                 |
  |                                                                      |
  +----------------------------------------------------------------------+
*/

dl("monetdb.so");

$db = monetdb_connect("sql");
$h = monetdb_query("SELECT name from tables; SELECT name from tables limit 2;");

do {
    echo "-- RESULT SET ".(++$i).":\n";
    while ($row = monetdb_fetch_row($h)) {
	echo $row[0]."\n";
    }
} while (monetdb_next_result($h));

?>
