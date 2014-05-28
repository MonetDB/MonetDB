<?php
require 'monetdb/php_monetdb.php';

$db = monetdb_connect("sql", "localhost", $argv[1], "monetdb", "monetdb", $argv[2]) or die(monetdb_last_error());

$res = monetdb_query('START TRANSACTION;') or die(monetdb_last_error());
while ( $row = monetdb_fetch_assoc($res) ) { print_r($row); }

$res = monetdb_query('CREATE TABLE php_int128 (i HUGEINT);') or die(monetdb_last_error());
while ( $row = monetdb_fetch_assoc($res) ) { print_r($row); }

$res = monetdb_query('INSERT INTO php_int128 VALUES (123456789098765432101234567890987654321);') or die(monetdb_last_error());
while ( $row = monetdb_fetch_assoc($res) ) { print_r($row); }

$res = monetdb_query('SELECT * FROM php_int128;') or die(monetdb_last_error());
while ( $row = monetdb_fetch_assoc($res) ) { print_r($row); }

$res = monetdb_query('SELECT * FROM php_int128;') or die(monetdb_last_error());
$cols = monetdb_num_fields($res);
for ($i = 0; $i < $cols; $i++) {
	print(monetdb_field_name($res, $i)."\t");
}
print("\n");
while ($row = @monetdb_fetch_row($res)) {
	for ($i = 0; $i < $cols; $i++) {
		print($row[$i]."\t");
	}
	print("\n");
}
?>
