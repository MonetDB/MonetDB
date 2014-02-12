#!/usr/bin/php 

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

<?php
require 'monetdb/php_monetdb.php';

$db = monetdb_connect("sql", "localhost", $argv[1], "monetdb", "monetdb", $argv[2]);
$tables = monetdb_query('SELECT name FROM tables');

for ($i = 0; $line = @monetdb_fetch_assoc($tables); $i++) {
	print($line['name']."\n");
}
$result = monetdb_query('SELECT name, schema_id, query, type, system, commit_action, readonly, temporary FROM tables');
$cols = monetdb_num_fields($result);
for ($i = 0; $i < $cols; $i++) {
	print(monetdb_field_name($result, $i)."\t");
}
print("\n");
while ($row = @monetdb_fetch_row($result)) {
	for ($i = 0; $i < $cols; $i++) {
		print($row[$i]."\t");
	}
	print("\n");
}
?>
