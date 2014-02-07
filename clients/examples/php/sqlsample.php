#!/usr/bin/php 
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
