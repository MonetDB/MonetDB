#!/usr/bin/php 

<?php
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
?>

<?php
require 'monetdb/php_monetdb.php';

$db = monetdb_connect("sql", "localhost", $argv[1], "monetdb", "monetdb", $argv[2]);
$tables = monetdb_query('SELECT name FROM tables LIMIT 10');

for ($i = 0; $line = @monetdb_fetch_assoc($tables); $i++) {
	print($line['name']."\n");
}
$result = monetdb_query('SELECT name, schema_id, query, type, system, commit_action, access, temporary FROM tables LIMIT 10');
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
